package org.apache.rocketmq.dleger.store.file;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.entry.DLegerEntryCoder;
import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerMmapFileStore extends DLegerStore {

    private static Logger logger = LoggerFactory.getLogger(DLegerMmapFileStore.class);

    public static final int MAGIC_1 = 1;
    public static final int CURRENT_MAGIC = MAGIC_1;

    public static final int INDEX_NUIT_SIZE = 32;

    private long legerBeginIndex = -1;
    private long legerEndIndex = -1;
    private long committedIndex = -1;
    private long legerEndTerm;

    private DLegerConfig dLegerConfig;
    private MemberState memberState;

    private MmapFileQueue dataFileQueue;
    private MmapFileQueue indexFileQueue;

    private ThreadLocal<ByteBuffer> localEntryBuffer;
    private ThreadLocal<ByteBuffer> localIndexBuffer;


    public DLegerMmapFileStore(DLegerConfig dLegerConfig, MemberState memberState) {
        this.dLegerConfig = dLegerConfig;
        this.memberState = memberState;
        this.dataFileQueue = new MmapFileQueue(dLegerConfig.getDataStorePath(), dLegerConfig.getMappedFileSizeForEntryData());
        this.indexFileQueue = new MmapFileQueue(dLegerConfig.getIndexStorePath(), dLegerConfig.getMappedFileSizeForEntryIndex());
        localEntryBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024 * 1024));
        localIndexBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INDEX_NUIT_SIZE * 2));
    }


    public void startup() {
        this.dataFileQueue.load();
        this.indexFileQueue.load();
        PreConditions.check(dataFileQueue.checkSelf(), DLegerException.Code.DISK_ERROR, "check data file order failed before recovery");
        PreConditions.check(indexFileQueue.checkSelf(), DLegerException.Code.DISK_ERROR, "check index file order failed before recovery");
        recover();
        PreConditions.check(dataFileQueue.checkSelf(), DLegerException.Code.DISK_ERROR, "check data file order failed after recovery");
        PreConditions.check(indexFileQueue.checkSelf(), DLegerException.Code.DISK_ERROR, "check index file order failed after recovery");
    }

    public void shutdown() {
        this.dataFileQueue.flush(0);
        this.indexFileQueue.flush(0);
    }


    public long getWritePos() {
        return dataFileQueue.getMaxWrotePosition();
    }
    public long getFlushPos() {
        return dataFileQueue.getFlushedWhere();
    }
    public void flush() {
        this.dataFileQueue.flush(0);
        this.indexFileQueue.flush(0);
    }

    public void recover() {
        final List<MmapFile> mappedFiles = this.dataFileQueue.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            this.indexFileQueue.updateWherePosition(0);
            this.indexFileQueue.truncateDirtyFiles(0);
            return;
        }
        int index = mappedFiles.size() - 3;
        if (index < 0) {
            index = 0;
        }

        long firstEntryIndex = -1;
        for (int i = index; i >= 0; i--) {
            index = i;
            MmapFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            try {
                int magic = byteBuffer.getInt();
                int size = byteBuffer.getInt();
                long entryIndex = byteBuffer.getLong();
                long entryTerm = byteBuffer.get();
                PreConditions.check(magic != MmapFileQueue.BLANK_MAGIC_CODE && magic >= MAGIC_1 && MAGIC_1 <= CURRENT_MAGIC, DLegerException.Code.DISK_ERROR, "unknown magic is " + magic);
                PreConditions.check(size > DLegerEntry.HEADER_SIZE, DLegerException.Code.DISK_ERROR, String.format("Size %d should greater than %d", size, DLegerEntry.HEADER_SIZE) );

                SelectMmapBufferResult indexSbr = indexFileQueue.getData(entryIndex * INDEX_NUIT_SIZE);
                PreConditions.check(indexSbr != null, DLegerException.Code.DISK_ERROR, String.format("index: %d pos: %d", entryIndex, entryIndex * INDEX_NUIT_SIZE));
                indexSbr.release();
                ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
                int magicFromIndex = indexByteBuffer.getInt();
                long posFromIndex = indexByteBuffer.getLong();
                int sizeFromIndex = indexByteBuffer.getInt();
                long indexFromIndex = indexByteBuffer.getLong();
                long termFromIndex = indexByteBuffer.get();
                PreConditions.check(magic == magicFromIndex, DLegerException.Code.DISK_ERROR, String.format("magic %d != %d", magic, magicFromIndex));
                PreConditions.check(size == sizeFromIndex, DLegerException.Code.DISK_ERROR, String.format("size %d != %d", size, sizeFromIndex));
                PreConditions.check(entryIndex == indexFromIndex, DLegerException.Code.DISK_ERROR, String.format("index %d != %d", entryIndex, indexFromIndex));
                PreConditions.check(entryTerm == termFromIndex, DLegerException.Code.DISK_ERROR, String.format("term %d != %d", entryTerm, termFromIndex));
                PreConditions.check(posFromIndex == mappedFile.getFileFromOffset(), DLegerException.Code.DISK_ERROR, String.format("pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex));
                firstEntryIndex = entryIndex;
                break;
            } catch (Throwable t) {
                logger.warn("Pre check data and index failed {}", mappedFile.getFileName(), t);
            }
        }

        MmapFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        logger.info("Begin to recover data from entryIndex: {} fileIndex: {} fileSize: {} fileName:{} ", firstEntryIndex, index, mappedFiles.size(), mappedFile.getFileName());
        long lastEntryIndex = -1;
        long lastEntryTerm = -1;
        long processOffset = mappedFile.getFileFromOffset();
        boolean needWriteIndex = false;
        while (true) {
            try {
                int relativePos = byteBuffer.position();
                long absolutePos = mappedFile.getFileFromOffset() + relativePos;
                int magic = byteBuffer.getInt();
                if (magic == MmapFileQueue.BLANK_MAGIC_CODE) {
                    processOffset =  mappedFile.getFileFromOffset() + mappedFile.getFileSize();
                    index++;
                    if (index >= mappedFiles.size()) {
                        logger.info("Recover data file over, the last file {}", mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        logger.info("Trying to recover index file {}", mappedFile.getFileName());
                        continue;
                    }
                }

                int size = byteBuffer.getInt();
                long entryIndex = byteBuffer.getLong();
                long entryTerm = byteBuffer.get();
                byteBuffer.position(relativePos + size);

                String message = String.format("pos: %d size: %d magic:%d index:%d term:%d", absolutePos, size, magic, entryIndex, entryTerm);
                PreConditions.check(magic <= CURRENT_MAGIC && magic >= MAGIC_1, DLegerException.Code.DISK_ERROR, String.format("%s currMagic: %d", message, CURRENT_MAGIC));
                if (lastEntryIndex != -1) {
                    PreConditions.check(entryIndex == lastEntryIndex + 1, DLegerException.Code.DISK_ERROR, String.format("%s lastEntryIndex: %d", message, lastEntryIndex));
                }
                PreConditions.check(entryTerm >= lastEntryTerm, DLegerException.Code.DISK_ERROR, String.format("%s lastEntryTerm: ", message, lastEntryTerm));
                PreConditions.check(size > DLegerEntry.HEADER_SIZE, DLegerException.Code.DISK_ERROR, String.format("Size %d should greater than %d", size, DLegerEntry.HEADER_SIZE) );
                if (!needWriteIndex) {
                    try {
                        SelectMmapBufferResult indexSbr = indexFileQueue.getData(entryIndex * INDEX_NUIT_SIZE);
                        PreConditions.check(indexSbr != null, DLegerException.Code.DISK_ERROR, String.format("index: %d pos: %d", entryIndex, entryIndex * INDEX_NUIT_SIZE));
                        indexSbr.release();
                        ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
                        int magicFromIndex = indexByteBuffer.getInt();
                        long posFromIndex = indexByteBuffer.getLong();
                        int sizeFromIndex = indexByteBuffer.getInt();
                        long indexFromIndex = indexByteBuffer.getLong();
                        long termFromIndex = indexByteBuffer.get();
                        PreConditions.check(magic == magicFromIndex, DLegerException.Code.DISK_ERROR, String.format("magic %d != %d", magic, magicFromIndex));
                        PreConditions.check(size == sizeFromIndex, DLegerException.Code.DISK_ERROR, String.format("size %d != %d", size, sizeFromIndex));
                        PreConditions.check(entryIndex == indexFromIndex, DLegerException.Code.DISK_ERROR, String.format("index %d != %d", entryIndex, indexFromIndex));
                        PreConditions.check(entryTerm == termFromIndex, DLegerException.Code.DISK_ERROR, String.format("term %d != %d", entryTerm, termFromIndex));
                        PreConditions.check(absolutePos == posFromIndex, DLegerException.Code.DISK_ERROR, String.format("pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex));
                    } catch (Exception e) {
                        logger.warn("Compare data to index failed {}", mappedFile.getFileName());
                        indexFileQueue.truncateDirtyFiles(entryIndex * INDEX_NUIT_SIZE);
                        if (indexFileQueue.getMaxWrotePosition() != entryIndex * INDEX_NUIT_SIZE) {
                            logger.warn("Unexpected wrote position in index file {} != {}", indexFileQueue.getMaxWrotePosition(), entryIndex * INDEX_NUIT_SIZE);
                            indexFileQueue.truncateDirtyFiles(0);
                        }
                        if (indexFileQueue.getMappedFiles().isEmpty()) {
                            indexFileQueue.getLastMappedFile(entryIndex * INDEX_NUIT_SIZE);
                        }
                        needWriteIndex = true;
                    }
                }
                if (needWriteIndex) {
                    ByteBuffer indexBuffer = localIndexBuffer.get();
                    DLegerEntryCoder.encodeIndex(absolutePos, size, magic, entryIndex, entryTerm, indexBuffer);
                    long indexPos = indexFileQueue.append(indexBuffer.array(), 0, indexBuffer.remaining());
                    PreConditions.check(indexPos == entryIndex * INDEX_NUIT_SIZE, DLegerException.Code.DISK_ERROR, String.format("Write index failed index: %d", entryIndex));
                }
                lastEntryIndex = entryIndex;
                lastEntryTerm = entryTerm;
                processOffset += size;
            } catch (Throwable t) {
                logger.info("Recover data file to the end of {} ", mappedFile.getFileName(), t);
                break;
            }
        }
        logger.info("Recover data to the end entryIndex:{} processOffset:{}", lastEntryIndex, processOffset);
        legerEndIndex = lastEntryIndex;
        legerEndTerm = lastEntryTerm;
        if (lastEntryIndex != -1) {
            DLegerEntry entry = get(lastEntryIndex);
            PreConditions.check(entry != null, DLegerException.Code.DISK_ERROR, "recheck get null entry");
            PreConditions.check(entry.getIndex() == lastEntryIndex, DLegerException.Code.DISK_ERROR, String.format("recheck index %d != %d", entry.getIndex(), lastEntryIndex));
            //get leger begin index
            ByteBuffer tmpBuffer = dataFileQueue.getFirstMappedFile().sliceByteBuffer();
            tmpBuffer.getInt(); //magic
            tmpBuffer.getInt(); //size
            legerBeginIndex = byteBuffer.getLong();
        } else {
            processOffset = 0;
        }
        this.dataFileQueue.updateWherePosition(processOffset);
        this.dataFileQueue.truncateDirtyFiles(processOffset);
        long indexProcessOffset = (lastEntryIndex + 1) * INDEX_NUIT_SIZE;
        this.indexFileQueue.updateWherePosition(indexProcessOffset);
        this.indexFileQueue.truncateDirtyFiles(indexProcessOffset);
        return;
    }

    @Override
    public long appendAsLeader(DLegerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLegerException.Code.NOT_LEADER, null, memberState.getLeaderId());
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLegerEntryCoder.encode(entry, dataBuffer);
        int entrySize =  dataBuffer.remaining();
        synchronized (memberState) {
            //TODO handle disk error
            long nextIndex = legerEndIndex + 1;
            PreConditions.check(memberState.isLeader(), DLegerException.Code.NOT_LEADER, null, memberState.getLeaderId());
            entry.setIndex(nextIndex);
            entry.setTerm(memberState.currTerm());
            entry.setMagic(CURRENT_MAGIC);
            DLegerEntryCoder.setIndexTerm(dataBuffer, nextIndex, memberState.currTerm(), CURRENT_MAGIC);
            long dataPos = dataFileQueue.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos != -1, DLegerException.Code.DISK_ERROR, null);
            DLegerEntryCoder.encodeIndex(dataPos, entrySize, CURRENT_MAGIC, nextIndex, memberState.currTerm(), indexBuffer);
            long indexPos = indexFileQueue.append(indexBuffer.array(), 0, indexBuffer.remaining());
            PreConditions.check(indexPos == entry.getIndex() * INDEX_NUIT_SIZE, DLegerException.Code.DISK_ERROR, null);
            if (logger.isDebugEnabled()) {
                logger.info("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            legerEndIndex++;
            committedIndex++;
            legerEndTerm = memberState.currTerm();
            if (legerBeginIndex == -1) {
                legerBeginIndex = legerEndIndex;
            }
            return legerEndIndex;
        }
    }


    @Override
    public long appendAsFollower(DLegerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLegerException.Code.NOT_FOLLOWER, null, memberState.getLeaderId());
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLegerEntryCoder.encode(entry, dataBuffer);
        int entrySize = dataBuffer.remaining();
        synchronized(memberState) {
            long nextIndex = legerEndIndex + 1;
            PreConditions.check(nextIndex ==  entry.getIndex(), DLegerException.Code.UNCONSISTENCT_INDEX, null, memberState.getLeaderId());
            PreConditions.check(memberState.isFollower(), DLegerException.Code.NOT_FOLLOWER, null, memberState.getLeaderId());
            PreConditions.check(leaderTerm == memberState.currTerm(), DLegerException.Code.UNCONSISTENCT_TERM, null, memberState.getLeaderId());
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLegerException.Code.UNCONSISTENCT_LEADER, null, memberState.getLeaderId());
            long dataPos = dataFileQueue.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos != -1, DLegerException.Code.DISK_ERROR, null);
            DLegerEntryCoder.encodeIndex(dataPos, entrySize, entry.getMagic(), entry.getIndex(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileQueue.append(indexBuffer.array(), 0, indexBuffer.remaining());
            PreConditions.check(indexPos == entry.getIndex() * INDEX_NUIT_SIZE, DLegerException.Code.DISK_ERROR, null);
            legerEndTerm = memberState.currTerm();
            legerEndIndex = entry.getIndex();
            committedIndex = entry.getIndex();
            if (legerBeginIndex == -1) {
                legerBeginIndex = legerEndIndex;
            }
            return entry.getIndex();
        }

    }

    public long getLegerEndIndex() {
        return legerEndIndex;
    }

    @Override public long getLegerBeginIndex() {
        return legerBeginIndex;
    }

    @Override
    public DLegerEntry get(Long index) {
        PreConditions.check(index <= legerEndIndex && index >= legerBeginIndex, DLegerException.Code.INDEX_OUT_OF_RANGE, String.format("%d should between %d-%d", index, legerBeginIndex, legerEndIndex), memberState.getLeaderId());
        SelectMmapBufferResult indexSbr = indexFileQueue.getData(index * INDEX_NUIT_SIZE, INDEX_NUIT_SIZE);
        PreConditions.check(indexSbr.getByteBuffer() != null, DLegerException.Code.DISK_ERROR, null);
        indexSbr.getByteBuffer().getInt(); //magic
        long pos = indexSbr.getByteBuffer().getLong();
        int size = indexSbr.getByteBuffer().getInt();
        indexSbr.release();
        SelectMmapBufferResult dataSbr = dataFileQueue.getData(pos, size);
        PreConditions.check(dataSbr.getByteBuffer() != null, DLegerException.Code.DISK_ERROR, null);
        DLegerEntry dLegerEntry = DLegerEntryCoder.decode(dataSbr.getByteBuffer());
        dataSbr.release();
        return dLegerEntry;
    }


    public long getCommittedIndex() {
        return committedIndex;
    }

    public long getLegerEndTerm() {
        return legerEndTerm;
    }
}
