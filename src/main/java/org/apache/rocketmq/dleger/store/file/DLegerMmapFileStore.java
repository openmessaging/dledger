package org.apache.rocketmq.dleger.store.file;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.entry.DLegerEntryCoder;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
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

    private MmapFileList dataFileList;
    private MmapFileList indexFileList;

    private ThreadLocal<ByteBuffer> localEntryBuffer;
    private ThreadLocal<ByteBuffer> localIndexBuffer;


    public DLegerMmapFileStore(DLegerConfig dLegerConfig, MemberState memberState) {
        this.dLegerConfig = dLegerConfig;
        this.memberState = memberState;
        this.dataFileList = new MmapFileList(dLegerConfig.getDataStorePath(), dLegerConfig.getMappedFileSizeForEntryData());
        this.indexFileList = new MmapFileList(dLegerConfig.getIndexStorePath(), dLegerConfig.getMappedFileSizeForEntryIndex());
        localEntryBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024 * 1024));
        localIndexBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INDEX_NUIT_SIZE * 2));
    }


    public void startup() {
        this.dataFileList.load();
        this.indexFileList.load();
        PreConditions.check(dataFileList.checkSelf(), DLegerResponseCode.DISK_ERROR, "check data file order failed before recovery");
        PreConditions.check(indexFileList.checkSelf(), DLegerResponseCode.DISK_ERROR, "check index file order failed before recovery");
        recover();
        PreConditions.check(dataFileList.checkSelf(), DLegerResponseCode.DISK_ERROR, "check data file order failed after recovery");
        PreConditions.check(indexFileList.checkSelf(), DLegerResponseCode.DISK_ERROR, "check index file order failed after recovery");
    }

    public void shutdown() {
        this.dataFileList.flush(0);
        this.indexFileList.flush(0);
    }


    public long getWritePos() {
        return dataFileList.getMaxWrotePosition();
    }
    public long getFlushPos() {
        return dataFileList.getFlushedWhere();
    }
    public void flush() {
        this.dataFileList.flush(0);
        this.indexFileList.flush(0);
    }

    public void recover() {
        final List<MmapFile> mappedFiles = this.dataFileList.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            this.indexFileList.updateWherePosition(0);
            this.indexFileList.truncateOffset(0);
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
                PreConditions.check(magic != MmapFileList.BLANK_MAGIC_CODE && magic >= MAGIC_1 && MAGIC_1 <= CURRENT_MAGIC, DLegerResponseCode.DISK_ERROR, "unknown magic is " + magic);
                PreConditions.check(size > DLegerEntry.HEADER_SIZE, DLegerResponseCode.DISK_ERROR, String.format("Size %d should greater than %d", size, DLegerEntry.HEADER_SIZE) );

                SelectMmapBufferResult indexSbr = indexFileList.getData(entryIndex * INDEX_NUIT_SIZE);
                PreConditions.check(indexSbr != null, DLegerResponseCode.DISK_ERROR, String.format("index: %d pos: %d", entryIndex, entryIndex * INDEX_NUIT_SIZE));
                indexSbr.release();
                ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
                int magicFromIndex = indexByteBuffer.getInt();
                long posFromIndex = indexByteBuffer.getLong();
                int sizeFromIndex = indexByteBuffer.getInt();
                long indexFromIndex = indexByteBuffer.getLong();
                long termFromIndex = indexByteBuffer.get();
                PreConditions.check(magic == magicFromIndex, DLegerResponseCode.DISK_ERROR, String.format("magic %d != %d", magic, magicFromIndex));
                PreConditions.check(size == sizeFromIndex, DLegerResponseCode.DISK_ERROR, String.format("size %d != %d", size, sizeFromIndex));
                PreConditions.check(entryIndex == indexFromIndex, DLegerResponseCode.DISK_ERROR, String.format("index %d != %d", entryIndex, indexFromIndex));
                PreConditions.check(entryTerm == termFromIndex, DLegerResponseCode.DISK_ERROR, String.format("term %d != %d", entryTerm, termFromIndex));
                PreConditions.check(posFromIndex == mappedFile.getFileFromOffset(), DLegerResponseCode.DISK_ERROR, String.format("pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex));
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
                if (magic == MmapFileList.BLANK_MAGIC_CODE) {
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
                PreConditions.check(magic <= CURRENT_MAGIC && magic >= MAGIC_1, DLegerResponseCode.DISK_ERROR, String.format("%s currMagic: %d", message, CURRENT_MAGIC));
                if (lastEntryIndex != -1) {
                    PreConditions.check(entryIndex == lastEntryIndex + 1, DLegerResponseCode.DISK_ERROR, String.format("%s lastEntryIndex: %d", message, lastEntryIndex));
                }
                PreConditions.check(entryTerm >= lastEntryTerm, DLegerResponseCode.DISK_ERROR, String.format("%s lastEntryTerm: ", message, lastEntryTerm));
                PreConditions.check(size > DLegerEntry.HEADER_SIZE, DLegerResponseCode.DISK_ERROR, String.format("Size %d should greater than %d", size, DLegerEntry.HEADER_SIZE) );
                if (!needWriteIndex) {
                    try {
                        SelectMmapBufferResult indexSbr = indexFileList.getData(entryIndex * INDEX_NUIT_SIZE);
                        PreConditions.check(indexSbr != null, DLegerResponseCode.DISK_ERROR, String.format("index: %d pos: %d", entryIndex, entryIndex * INDEX_NUIT_SIZE));
                        indexSbr.release();
                        ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
                        int magicFromIndex = indexByteBuffer.getInt();
                        long posFromIndex = indexByteBuffer.getLong();
                        int sizeFromIndex = indexByteBuffer.getInt();
                        long indexFromIndex = indexByteBuffer.getLong();
                        long termFromIndex = indexByteBuffer.get();
                        PreConditions.check(magic == magicFromIndex, DLegerResponseCode.DISK_ERROR, String.format("magic %d != %d", magic, magicFromIndex));
                        PreConditions.check(size == sizeFromIndex, DLegerResponseCode.DISK_ERROR, String.format("size %d != %d", size, sizeFromIndex));
                        PreConditions.check(entryIndex == indexFromIndex, DLegerResponseCode.DISK_ERROR, String.format("index %d != %d", entryIndex, indexFromIndex));
                        PreConditions.check(entryTerm == termFromIndex, DLegerResponseCode.DISK_ERROR, String.format("term %d != %d", entryTerm, termFromIndex));
                        PreConditions.check(absolutePos == posFromIndex, DLegerResponseCode.DISK_ERROR, String.format("pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex));
                    } catch (Exception e) {
                        logger.warn("Compare data to index failed {}", mappedFile.getFileName());
                        indexFileList.truncateOffset(entryIndex * INDEX_NUIT_SIZE);
                        if (indexFileList.getMaxWrotePosition() != entryIndex * INDEX_NUIT_SIZE) {
                            long truncateIndexOffset = entryIndex * INDEX_NUIT_SIZE;
                            logger.warn("[Recovery] rebuild for index wrotePos: {} != truncatePos: {}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
                            PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLegerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
                        }
                        needWriteIndex = true;
                    }
                }
                if (needWriteIndex) {
                    ByteBuffer indexBuffer = localIndexBuffer.get();
                    DLegerEntryCoder.encodeIndex(absolutePos, size, magic, entryIndex, entryTerm, indexBuffer);
                    long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
                    PreConditions.check(indexPos == entryIndex * INDEX_NUIT_SIZE, DLegerResponseCode.DISK_ERROR, String.format("Write index failed index: %d", entryIndex));
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
            PreConditions.check(entry != null, DLegerResponseCode.DISK_ERROR, "recheck get null entry");
            PreConditions.check(entry.getIndex() == lastEntryIndex, DLegerResponseCode.DISK_ERROR, String.format("recheck index %d != %d", entry.getIndex(), lastEntryIndex));
            reviseLegerBeginIndex();
        } else {
            processOffset = 0;
        }
        this.dataFileList.updateWherePosition(processOffset);
        this.dataFileList.truncateOffset(processOffset);
        long indexProcessOffset = (lastEntryIndex + 1) * INDEX_NUIT_SIZE;
        this.indexFileList.updateWherePosition(indexProcessOffset);
        this.indexFileList.truncateOffset(indexProcessOffset);
        return;
    }

    private void reviseLegerBeginIndex() {
        //get leger begin index
        MmapFile firstFile = dataFileList.getFirstMappedFile();
        ByteBuffer tmpBuffer = firstFile.sliceByteBuffer();
        tmpBuffer.position(firstFile.getStartPosition());
        tmpBuffer.getInt(); //magic
        tmpBuffer.getInt(); //size
        legerBeginIndex = tmpBuffer.getLong();
    }

    @Override
    public DLegerEntry appendAsLeader(DLegerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLegerResponseCode.NOT_LEADER, null);
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLegerEntryCoder.encode(entry, dataBuffer);
        int entrySize =  dataBuffer.remaining();
        synchronized (memberState) {
            long nextIndex = legerEndIndex + 1;
            PreConditions.check(memberState.isLeader(), DLegerResponseCode.NOT_LEADER, null);
            entry.setIndex(nextIndex);
            entry.setTerm(memberState.currTerm());
            entry.setMagic(CURRENT_MAGIC);
            DLegerEntryCoder.setIndexTerm(dataBuffer, nextIndex, memberState.currTerm(), CURRENT_MAGIC);
            long dataPos = dataFileList.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos != -1, DLegerResponseCode.DISK_ERROR, null);
            DLegerEntryCoder.encodeIndex(dataPos, entrySize, CURRENT_MAGIC, nextIndex, memberState.currTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_NUIT_SIZE, DLegerResponseCode.DISK_ERROR, null);
            if (logger.isDebugEnabled()) {
                logger.info("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            legerEndIndex++;
            legerEndTerm = memberState.currTerm();
            if (legerBeginIndex == -1) {
                legerBeginIndex = legerEndIndex;
            }
            return entry;
        }
    }




    @Override
    public long truncate(DLegerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLegerResponseCode.NOT_FOLLOWER, null);
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLegerEntryCoder.encode(entry, dataBuffer);
        int entrySize = dataBuffer.remaining();
        synchronized(memberState) {
            PreConditions.check(memberState.isFollower(), DLegerResponseCode.NOT_FOLLOWER, "role=%s", memberState.getRole());
            PreConditions.check(leaderTerm == memberState.currTerm(), DLegerResponseCode.INCONSISTENT_TERM, "term %d != %d", leaderTerm, memberState.currTerm());
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLegerResponseCode.INCONSISTENT_LEADER, "leaderId %s != %s", leaderId, memberState.getLeaderId());
            boolean existedEntry;
            try {
                DLegerEntry tmp = get(entry.getIndex());
                existedEntry = entry.equals(tmp);
            } catch (Throwable ignored) {
                existedEntry = false;
            }
            long truncatePos = existedEntry ? entry.getPos() + entry.getSize() : entry.getPos();
            dataFileList.truncateOffset(truncatePos);
            if (dataFileList.getMaxWrotePosition() != truncatePos) {
                logger.warn("[TRUNCATE] rebuild for data wrotePos: {} != truncatePos: {}", dataFileList.getMaxWrotePosition(), truncatePos);
                PreConditions.check(dataFileList.rebuildWithPos(truncatePos), DLegerResponseCode.DISK_ERROR, "rebuild data truncatePos=%d", truncatePos);
            }
            if (!existedEntry) {
                long dataPos = dataFileList.append(dataBuffer.array(), 0, dataBuffer.remaining());
                PreConditions.check(dataPos == entry.getPos(), DLegerResponseCode.DISK_ERROR, " %d != %d", dataPos, entry.getPos());
            }

            long truncateIndexOffset =  entry.getIndex() * INDEX_NUIT_SIZE;
            indexFileList.truncateOffset(truncateIndexOffset);
            if (indexFileList.getMaxWrotePosition() != truncateIndexOffset) {
                logger.warn("[TRUNCATE] rebuild for index wrotePos: {} != truncatePos: {}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
                PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLegerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
            }
            DLegerEntryCoder.encodeIndex(entry.getPos(), entrySize, entry.getMagic(), entry.getIndex(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_NUIT_SIZE, DLegerResponseCode.DISK_ERROR, null);
            legerEndTerm = memberState.currTerm();
            legerEndIndex = entry.getIndex();
            committedIndex = entry.getIndex();
            reviseLegerBeginIndex();
            return entry.getIndex();
        }
    }




    @Override
    public DLegerEntry appendAsFollower(DLegerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLegerResponseCode.NOT_FOLLOWER, "role=%s", memberState.getRole());
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLegerEntryCoder.encode(entry, dataBuffer);
        int entrySize = dataBuffer.remaining();
        synchronized(memberState) {
            PreConditions.check(memberState.isFollower(), DLegerResponseCode.NOT_FOLLOWER, "role=%s", memberState.getRole());
            long nextIndex = legerEndIndex + 1;
            PreConditions.check(nextIndex ==  entry.getIndex(), DLegerResponseCode.INCONSISTENT_INDEX, null);
            PreConditions.check(leaderTerm == memberState.currTerm(), DLegerResponseCode.INCONSISTENT_TERM, null);
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLegerResponseCode.INCONSISTENT_LEADER, null);
            long dataPos = dataFileList.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos == entry.getPos(), DLegerResponseCode.DISK_ERROR, String.format("%d != %d", dataPos, entry.getPos()));
            DLegerEntryCoder.encodeIndex(dataPos, entrySize, entry.getMagic(), entry.getIndex(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_NUIT_SIZE, DLegerResponseCode.DISK_ERROR, null);
            legerEndTerm = memberState.currTerm();
            legerEndIndex = entry.getIndex();
            committedIndex = entry.getIndex();
            if (legerBeginIndex == -1) {
                legerBeginIndex = legerEndIndex;
            }
            return entry;
        }

    }

    @Override
    public long getLegerEndIndex() {
        return legerEndIndex;
    }

    @Override public long getLegerBeginIndex() {
        return legerBeginIndex;
    }

    @Override
    public DLegerEntry get(Long index) {

        PreConditions.check(index >= 0, DLegerResponseCode.INDEX_OUT_OF_RANGE, String.format("%d should gt 0", index));
        PreConditions.check(index <= legerEndIndex && index >= legerBeginIndex, DLegerResponseCode.INDEX_OUT_OF_RANGE, String.format("%d should between %d-%d", index, legerBeginIndex, legerEndIndex));
        SelectMmapBufferResult indexSbr = indexFileList.getData(index * INDEX_NUIT_SIZE, INDEX_NUIT_SIZE);
        PreConditions.check(indexSbr.getByteBuffer() != null, DLegerResponseCode.DISK_ERROR, null);
        indexSbr.getByteBuffer().getInt(); //magic
        long pos = indexSbr.getByteBuffer().getLong();
        int size = indexSbr.getByteBuffer().getInt();
        indexSbr.release();
        SelectMmapBufferResult dataSbr = dataFileList.getData(pos, size);
        PreConditions.check(dataSbr.getByteBuffer() != null, DLegerResponseCode.DISK_ERROR, null);
        DLegerEntry dLegerEntry = DLegerEntryCoder.decode(dataSbr.getByteBuffer());
        dLegerEntry.setPos(pos);
        dataSbr.release();
        return dLegerEntry;
    }


    @Override
    public long getCommittedIndex() {
        return committedIndex;
    }

    @Override
    public long getLegerEndTerm() {
        return legerEndTerm;
    }

    @Override
    public MemberState getMemberState() {
        return memberState;
    }

    public MmapFileList getDataFileList() {
        return dataFileList;
    }

    public MmapFileList getIndexFileList() {
        return indexFileList;
    }
}
