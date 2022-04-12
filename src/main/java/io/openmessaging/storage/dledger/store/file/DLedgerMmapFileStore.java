/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.store.file;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.ShutdownAbleThread;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.entry.DLedgerEntryCoder;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.utils.IOUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerMmapFileStore extends DLedgerStore {

    public static final String CHECK_POINT_FILE = "checkpoint";
    public static final String END_INDEX_KEY = "endIndex";
    public static final String COMMITTED_INDEX_KEY = "committedIndex";
    public static final int MAGIC_1 = 1;
    public static final int CURRENT_MAGIC = MAGIC_1;
    public static final int INDEX_UNIT_SIZE = 32;

    private static Logger logger = LoggerFactory.getLogger(DLedgerMmapFileStore.class);
    public List<AppendHook> appendHooks = new ArrayList<>();
    private long ledgerBeginIndex = -1;
    private long ledgerEndIndex = -1;
    private long committedIndex = -1;
    private long committedPos = -1;
    private long ledgerEndTerm;
    private DLedgerConfig dLedgerConfig;
    private MemberState memberState;
    private MmapFileList dataFileList;
    private MmapFileList indexFileList;
    private ThreadLocal<ByteBuffer> localEntryBuffer;
    private ThreadLocal<ByteBuffer> localIndexBuffer;
    private FlushDataService flushDataService;
    private CleanSpaceService cleanSpaceService;
    private volatile boolean isDiskFull = false;

    private long lastCheckPointTimeMs = System.currentTimeMillis();

    private AtomicBoolean hasLoaded = new AtomicBoolean(false);
    private AtomicBoolean hasRecovered = new AtomicBoolean(false);

    private volatile Set<String> fullStorePaths = Collections.emptySet();

    public DLedgerMmapFileStore(DLedgerConfig dLedgerConfig, MemberState memberState) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        if (dLedgerConfig.getDataStorePath().contains(DLedgerConfig.MULTI_PATH_SPLITTER)) {
            this.dataFileList = new MultiPathMmapFileList(dLedgerConfig, dLedgerConfig.getMappedFileSizeForEntryData(),
                    this::getFullStorePaths);
        } else {
            this.dataFileList = new MmapFileList(dLedgerConfig.getDataStorePath(), dLedgerConfig.getMappedFileSizeForEntryData());
        }
        this.indexFileList = new MmapFileList(dLedgerConfig.getIndexStorePath(), dLedgerConfig.getMappedFileSizeForEntryIndex());
        localEntryBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024 * 1024));
        localIndexBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(INDEX_UNIT_SIZE * 2));
        flushDataService = new FlushDataService("DLedgerFlushDataService", logger);
        cleanSpaceService = new CleanSpaceService("DLedgerCleanSpaceService", logger);
    }

    @Override
    public void startup() {
        load();
        recover();
        flushDataService.start();
        cleanSpaceService.start();
    }

    @Override
    public void shutdown() {
        this.dataFileList.flush(0);
        this.indexFileList.flush(0);
        persistCheckPoint();
        cleanSpaceService.shutdown();
        flushDataService.shutdown();
    }

    public long getWritePos() {
        return dataFileList.getMaxWrotePosition();
    }

    public long getFlushPos() {
        return dataFileList.getFlushedWhere();
    }

    @Override
    public void flush() {
        this.dataFileList.flush(0);
        this.indexFileList.flush(0);
    }

    public void load() {
        if (!hasLoaded.compareAndSet(false, true)) {
            return;
        }
        if (!this.dataFileList.load() || !this.indexFileList.load()) {
            logger.error("Load file failed, this usually indicates fatal error, you should check it manually");
            System.exit(-1);
        }
    }

    public void recover() {
        if (!hasRecovered.compareAndSet(false, true)) {
            return;
        }
        PreConditions.check(dataFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check data file order failed before recovery");
        PreConditions.check(indexFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check index file order failed before recovery");
        final List<MmapFile> mappedFiles = this.dataFileList.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            this.indexFileList.updateWherePosition(0);
            this.indexFileList.truncateOffset(0);
            return;
        }
        MmapFile lastMappedFile = dataFileList.getLastMappedFile();
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
                long startPos = mappedFile.getFileFromOffset();
                int magic = byteBuffer.getInt();
                int size = byteBuffer.getInt();
                long entryIndex = byteBuffer.getLong();
                long entryTerm = byteBuffer.getLong();
                long pos = byteBuffer.getLong();
                byteBuffer.getInt(); //channel
                byteBuffer.getInt(); //chain crc
                byteBuffer.getInt(); //body crc
                int bodySize = byteBuffer.getInt();
                PreConditions.check(magic != MmapFileList.BLANK_MAGIC_CODE && magic >= MAGIC_1 && MAGIC_1 <= CURRENT_MAGIC, DLedgerResponseCode.DISK_ERROR, "unknown magic=%d", magic);
                PreConditions.check(size > DLedgerEntry.HEADER_SIZE, DLedgerResponseCode.DISK_ERROR, "Size %d should > %d", size, DLedgerEntry.HEADER_SIZE);

                PreConditions.check(pos == startPos, DLedgerResponseCode.DISK_ERROR, "pos %d != %d", pos, startPos);
                PreConditions.check(bodySize + DLedgerEntry.BODY_OFFSET == size, DLedgerResponseCode.DISK_ERROR, "size %d != %d + %d", size, bodySize, DLedgerEntry.BODY_OFFSET);

                SelectMmapBufferResult indexSbr = indexFileList.getData(entryIndex * INDEX_UNIT_SIZE);
                PreConditions.check(indexSbr != null, DLedgerResponseCode.DISK_ERROR, "index=%d pos=%d", entryIndex, entryIndex * INDEX_UNIT_SIZE);
                indexSbr.release();
                ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
                int magicFromIndex = indexByteBuffer.getInt();
                long posFromIndex = indexByteBuffer.getLong();
                int sizeFromIndex = indexByteBuffer.getInt();
                long indexFromIndex = indexByteBuffer.getLong();
                long termFromIndex = indexByteBuffer.getLong();
                PreConditions.check(magic == magicFromIndex, DLedgerResponseCode.DISK_ERROR, "magic %d != %d", magic, magicFromIndex);
                PreConditions.check(size == sizeFromIndex, DLedgerResponseCode.DISK_ERROR, "size %d != %d", size, sizeFromIndex);
                PreConditions.check(entryIndex == indexFromIndex, DLedgerResponseCode.DISK_ERROR, "index %d != %d", entryIndex, indexFromIndex);
                PreConditions.check(entryTerm == termFromIndex, DLedgerResponseCode.DISK_ERROR, "term %d != %d", entryTerm, termFromIndex);
                PreConditions.check(posFromIndex == mappedFile.getFileFromOffset(), DLedgerResponseCode.DISK_ERROR, "pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex);
                firstEntryIndex = entryIndex;
                break;
            } catch (Throwable t) {
                logger.warn("Pre check data and index failed {}", mappedFile.getFileName(), t);
            }
        }

        MmapFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        logger.info("Begin to recover data from entryIndex={} fileIndex={} fileSize={} fileName={} ", firstEntryIndex, index, mappedFiles.size(), mappedFile.getFileName());
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
                    processOffset = mappedFile.getFileFromOffset() + mappedFile.getFileSize();
                    index++;
                    if (index >= mappedFiles.size()) {
                        logger.info("Recover data file over, the last file {}", mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        logger.info("Trying to recover data file {}", mappedFile.getFileName());
                        continue;
                    }
                }

                int size = byteBuffer.getInt();
                long entryIndex = byteBuffer.getLong();
                long entryTerm = byteBuffer.getLong();
                long pos = byteBuffer.getLong();
                byteBuffer.getInt(); //channel
                byteBuffer.getInt(); //chain crc
                byteBuffer.getInt(); //body crc
                int bodySize = byteBuffer.getInt();

                PreConditions.check(pos == absolutePos, DLedgerResponseCode.DISK_ERROR, "pos %d != %d", pos, absolutePos);
                PreConditions.check(bodySize + DLedgerEntry.BODY_OFFSET == size, DLedgerResponseCode.DISK_ERROR, "size %d != %d + %d", size, bodySize, DLedgerEntry.BODY_OFFSET);

                byteBuffer.position(relativePos + size);

                PreConditions.check(magic <= CURRENT_MAGIC && magic >= MAGIC_1, DLedgerResponseCode.DISK_ERROR, "pos=%d size=%d magic=%d index=%d term=%d currMagic=%d", absolutePos, size, magic, entryIndex, entryTerm, CURRENT_MAGIC);
                if (lastEntryIndex != -1) {
                    PreConditions.check(entryIndex == lastEntryIndex + 1, DLedgerResponseCode.DISK_ERROR, "pos=%d size=%d magic=%d index=%d term=%d lastEntryIndex=%d", absolutePos, size, magic, entryIndex, entryTerm, lastEntryIndex);
                }
                PreConditions.check(entryTerm >= lastEntryTerm, DLedgerResponseCode.DISK_ERROR, "pos=%d size=%d magic=%d index=%d term=%d lastEntryTerm=%d ", absolutePos, size, magic, entryIndex, entryTerm, lastEntryTerm);
                PreConditions.check(size > DLedgerEntry.HEADER_SIZE, DLedgerResponseCode.DISK_ERROR, "size %d should > %d", size, DLedgerEntry.HEADER_SIZE);
                if (!needWriteIndex) {
                    try {
                        SelectMmapBufferResult indexSbr = indexFileList.getData(entryIndex * INDEX_UNIT_SIZE);
                        PreConditions.check(indexSbr != null, DLedgerResponseCode.DISK_ERROR, "index=%d pos=%d", entryIndex, entryIndex * INDEX_UNIT_SIZE);
                        indexSbr.release();
                        ByteBuffer indexByteBuffer = indexSbr.getByteBuffer();
                        int magicFromIndex = indexByteBuffer.getInt();
                        long posFromIndex = indexByteBuffer.getLong();
                        int sizeFromIndex = indexByteBuffer.getInt();
                        long indexFromIndex = indexByteBuffer.getLong();
                        long termFromIndex = indexByteBuffer.getLong();
                        PreConditions.check(magic == magicFromIndex, DLedgerResponseCode.DISK_ERROR, "magic %d != %d", magic, magicFromIndex);
                        PreConditions.check(size == sizeFromIndex, DLedgerResponseCode.DISK_ERROR, "size %d != %d", size, sizeFromIndex);
                        PreConditions.check(entryIndex == indexFromIndex, DLedgerResponseCode.DISK_ERROR, "index %d != %d", entryIndex, indexFromIndex);
                        PreConditions.check(entryTerm == termFromIndex, DLedgerResponseCode.DISK_ERROR, "term %d != %d", entryTerm, termFromIndex);
                        PreConditions.check(absolutePos == posFromIndex, DLedgerResponseCode.DISK_ERROR, "pos %d != %d", mappedFile.getFileFromOffset(), posFromIndex);
                    } catch (Throwable t) {
                        logger.warn("Compare data to index failed {}", mappedFile.getFileName(), t);
                        indexFileList.truncateOffset(entryIndex * INDEX_UNIT_SIZE);
                        if (indexFileList.getMaxWrotePosition() != entryIndex * INDEX_UNIT_SIZE) {
                            long truncateIndexOffset = entryIndex * INDEX_UNIT_SIZE;
                            logger.warn("[Recovery] rebuild for index wrotePos={} not equal to truncatePos={}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
                            PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLedgerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
                        }
                        needWriteIndex = true;
                    }
                }
                if (needWriteIndex) {
                    ByteBuffer indexBuffer = localIndexBuffer.get();
                    DLedgerEntryCoder.encodeIndex(absolutePos, size, magic, entryIndex, entryTerm, indexBuffer);
                    long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
                    PreConditions.check(indexPos == entryIndex * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, "Write index failed index=%d", entryIndex);
                }
                lastEntryIndex = entryIndex;
                lastEntryTerm = entryTerm;
                processOffset += size;
            } catch (Throwable t) {
                logger.info("Recover data file to the end of {} ", mappedFile.getFileName(), t);
                break;
            }
        }
        logger.info("Recover data to the end entryIndex={} processOffset={} lastFileOffset={} cha={}",
            lastEntryIndex, processOffset, lastMappedFile.getFileFromOffset(), processOffset - lastMappedFile.getFileFromOffset());
        if (lastMappedFile.getFileFromOffset() - processOffset > lastMappedFile.getFileSize()) {
            logger.error("[MONITOR]The processOffset is too small, you should check it manually before truncating the data from {}", processOffset);
            System.exit(-1);
        }

        ledgerEndIndex = lastEntryIndex;
        ledgerEndTerm = lastEntryTerm;
        if (lastEntryIndex != -1) {
            DLedgerEntry entry = get(lastEntryIndex);
            PreConditions.check(entry != null, DLedgerResponseCode.DISK_ERROR, "recheck get null entry");
            PreConditions.check(entry.getIndex() == lastEntryIndex, DLedgerResponseCode.DISK_ERROR, "recheck index %d != %d", entry.getIndex(), lastEntryIndex);
            reviseLedgerBeginIndex();
        }
        this.dataFileList.updateWherePosition(processOffset);
        this.dataFileList.truncateOffset(processOffset);
        long indexProcessOffset = (lastEntryIndex + 1) * INDEX_UNIT_SIZE;
        this.indexFileList.updateWherePosition(indexProcessOffset);
        this.indexFileList.truncateOffset(indexProcessOffset);
        updateLedgerEndIndexAndTerm();
        PreConditions.check(dataFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check data file order failed after recovery");
        PreConditions.check(indexFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check index file order failed after recovery");
        //Load the committed index from checkpoint
        Properties properties = loadCheckPoint();
        if (properties == null || !properties.containsKey(COMMITTED_INDEX_KEY)) {
            return;
        }
        String committedIndexStr = String.valueOf(properties.get(COMMITTED_INDEX_KEY)).trim();
        if (committedIndexStr.length() <= 0) {
            return;
        }
        logger.info("Recover to get committed index={} from checkpoint", committedIndexStr);
        updateCommittedIndex(memberState.currTerm(), Long.valueOf(committedIndexStr));

    }

    private void reviseLedgerBeginIndex() {
        //get ledger begin index
        MmapFile firstFile = dataFileList.getFirstMappedFile();
        SelectMmapBufferResult sbr = firstFile.selectMappedBuffer(0);
        try {
            ByteBuffer tmpBuffer = sbr.getByteBuffer();
            tmpBuffer.position(firstFile.getStartPosition());
            tmpBuffer.getInt(); //magic
            tmpBuffer.getInt(); //size
            ledgerBeginIndex = tmpBuffer.getLong();
            indexFileList.resetOffset(ledgerBeginIndex * INDEX_UNIT_SIZE);
        } finally {
            SelectMmapBufferResult.release(sbr);
        }

    }

    @Override
    public DLedgerEntry appendAsLeader(DLedgerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
        PreConditions.check(!isDiskFull, DLedgerResponseCode.DISK_FULL);
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLedgerEntryCoder.encode(entry, dataBuffer);
        int entrySize = dataBuffer.remaining();
        synchronized (memberState) {
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER, null);
            PreConditions.check(memberState.getTransferee() == null, DLedgerResponseCode.LEADER_TRANSFERRING, null);
            long nextIndex = ledgerEndIndex + 1;
            entry.setIndex(nextIndex);
            entry.setTerm(memberState.currTerm());
            entry.setMagic(CURRENT_MAGIC);
            DLedgerEntryCoder.setIndexTerm(dataBuffer, nextIndex, memberState.currTerm(), CURRENT_MAGIC);
            long prePos = dataFileList.preAppend(dataBuffer.remaining());
            entry.setPos(prePos);
            PreConditions.check(prePos != -1, DLedgerResponseCode.DISK_ERROR, null);
            DLedgerEntryCoder.setPos(dataBuffer, prePos);
            for (AppendHook writeHook : appendHooks) {
                writeHook.doHook(entry, dataBuffer.slice(), DLedgerEntry.BODY_OFFSET);
            }
            long dataPos = dataFileList.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos != -1, DLedgerResponseCode.DISK_ERROR, null);
            PreConditions.check(dataPos == prePos, DLedgerResponseCode.DISK_ERROR, null);
            DLedgerEntryCoder.encodeIndex(dataPos, entrySize, CURRENT_MAGIC, nextIndex, memberState.currTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            if (logger.isDebugEnabled()) {
                logger.info("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            ledgerEndIndex++;
            ledgerEndTerm = memberState.currTerm();
            if (ledgerBeginIndex == -1) {
                ledgerBeginIndex = ledgerEndIndex;
            }
            updateLedgerEndIndexAndTerm();
            return entry;
        }
    }

    @Override
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER, null);
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLedgerEntryCoder.encode(entry, dataBuffer);
        int entrySize = dataBuffer.remaining();
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER, "role=%s", memberState.getRole());
            PreConditions.check(leaderTerm == memberState.currTerm(), DLedgerResponseCode.INCONSISTENT_TERM, "term %d != %d", leaderTerm, memberState.currTerm());
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER, "leaderId %s != %s", leaderId, memberState.getLeaderId());
            boolean existedEntry;
            try {
                DLedgerEntry tmp = get(entry.getIndex());
                existedEntry = entry.equals(tmp);
            } catch (Throwable ignored) {
                existedEntry = false;
            }
            long truncatePos = existedEntry ? entry.getPos() + entry.getSize() : entry.getPos();
            if (truncatePos != dataFileList.getMaxWrotePosition()) {
                logger.warn("[TRUNCATE]leaderId={} index={} truncatePos={} != maxPos={}, this is usually happened on the old leader", leaderId, entry.getIndex(), truncatePos, dataFileList.getMaxWrotePosition());
            }
            dataFileList.truncateOffset(truncatePos);
            if (dataFileList.getMaxWrotePosition() != truncatePos) {
                logger.warn("[TRUNCATE] rebuild for data wrotePos: {} != truncatePos: {}", dataFileList.getMaxWrotePosition(), truncatePos);
                PreConditions.check(dataFileList.rebuildWithPos(truncatePos), DLedgerResponseCode.DISK_ERROR, "rebuild data truncatePos=%d", truncatePos);
            }
            reviseDataFileListFlushedWhere(truncatePos);
            if (!existedEntry) {
                long dataPos = dataFileList.append(dataBuffer.array(), 0, dataBuffer.remaining());
                PreConditions.check(dataPos == entry.getPos(), DLedgerResponseCode.DISK_ERROR, " %d != %d", dataPos, entry.getPos());
            }

            long truncateIndexOffset = entry.getIndex() * INDEX_UNIT_SIZE;
            indexFileList.truncateOffset(truncateIndexOffset);
            if (indexFileList.getMaxWrotePosition() != truncateIndexOffset) {
                logger.warn("[TRUNCATE] rebuild for index wrotePos: {} != truncatePos: {}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
                PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLedgerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
            }
            reviseIndexFileListFlushedWhere(truncateIndexOffset);
            DLedgerEntryCoder.encodeIndex(entry.getPos(), entrySize, entry.getMagic(), entry.getIndex(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            ledgerEndTerm = entry.getTerm();
            ledgerEndIndex = entry.getIndex();
            reviseLedgerBeginIndex();
            updateLedgerEndIndexAndTerm();
            return entry.getIndex();
        }
    }

    private void reviseDataFileListFlushedWhere(long truncatePos) {
        long offset = calculateWherePosition(this.dataFileList, truncatePos);
        logger.info("Revise dataFileList flushedWhere from {} to {}", this.dataFileList.getFlushedWhere(), offset);
        // It seems unnecessary to set position atomically. Wrong position won't get updated during flush or commit.
        this.dataFileList.updateWherePosition(offset);
    }

    private void reviseIndexFileListFlushedWhere(long truncateIndexOffset) {
        long offset = calculateWherePosition(this.indexFileList, truncateIndexOffset);
        logger.info("Revise indexFileList flushedWhere from {} to {}", this.indexFileList.getFlushedWhere(), offset);
        this.indexFileList.updateWherePosition(offset);
    }

    /**
     * calculate wherePosition after truncate
     *
     * @param mappedFileList this.dataFileList or this.indexFileList
     * @param continuedBeginOffset new begining of offset
     */
    private long calculateWherePosition(final MmapFileList mappedFileList, long continuedBeginOffset) {
        if (mappedFileList.getFlushedWhere() == 0) {
            return 0;
        }
        if (mappedFileList.getMappedFiles().isEmpty()) {
            return continuedBeginOffset;
        }
        if (mappedFileList.getFlushedWhere() < mappedFileList.getFirstMappedFile().getFileFromOffset()) {
            return mappedFileList.getFirstMappedFile().getFileFromOffset();
        }

        // first from offset < continuedBeginOffset < flushedWhere
        if (mappedFileList.getFlushedWhere() > continuedBeginOffset) {
            return continuedBeginOffset;
        }

        return mappedFileList.getFlushedWhere();
    }

    @Override
    public DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER, "role=%s", memberState.getRole());
        PreConditions.check(!isDiskFull, DLedgerResponseCode.DISK_FULL);
        ByteBuffer dataBuffer = localEntryBuffer.get();
        ByteBuffer indexBuffer = localIndexBuffer.get();
        DLedgerEntryCoder.encode(entry, dataBuffer);
        int entrySize = dataBuffer.remaining();
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER, "role=%s", memberState.getRole());
            long nextIndex = ledgerEndIndex + 1;
            PreConditions.check(nextIndex == entry.getIndex(), DLedgerResponseCode.INCONSISTENT_INDEX, null);
            PreConditions.check(leaderTerm == memberState.currTerm(), DLedgerResponseCode.INCONSISTENT_TERM, null);
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER, null);
            long dataPos = dataFileList.append(dataBuffer.array(), 0, dataBuffer.remaining());
            PreConditions.check(dataPos == entry.getPos(), DLedgerResponseCode.DISK_ERROR, "%d != %d", dataPos, entry.getPos());
            DLedgerEntryCoder.encodeIndex(dataPos, entrySize, entry.getMagic(), entry.getIndex(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            ledgerEndTerm = entry.getTerm();
            ledgerEndIndex = entry.getIndex();
            if (ledgerBeginIndex == -1) {
                ledgerBeginIndex = ledgerEndIndex;
            }
            updateLedgerEndIndexAndTerm();
            return entry;
        }

    }

    void persistCheckPoint() {
        try {
            Properties properties = new Properties();
            properties.put(END_INDEX_KEY, getLedgerEndIndex());
            properties.put(COMMITTED_INDEX_KEY, getCommittedIndex());
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, dLedgerConfig.getDefaultPath() + File.separator + CHECK_POINT_FILE);
        } catch (Throwable t) {
            logger.error("Persist checkpoint failed", t);
        }
    }

    Properties loadCheckPoint() {
        try {
            String data = IOUtils.file2String(dLedgerConfig.getDefaultPath() + File.separator + CHECK_POINT_FILE);
            Properties properties = IOUtils.string2Properties(data);
            return properties;
        } catch (Throwable t) {
            logger.error("Load checkpoint failed", t);

        }
        return null;
    }

    @Override
    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    @Override public long getLedgerBeginIndex() {
        return ledgerBeginIndex;
    }

    @Override
    public DLedgerEntry get(Long index) {
        indexCheck(index);
        SelectMmapBufferResult indexSbr = null;
        SelectMmapBufferResult dataSbr = null;
        try {
            indexSbr = indexFileList.getData(index * INDEX_UNIT_SIZE, INDEX_UNIT_SIZE);
            PreConditions.check(indexSbr != null && indexSbr.getByteBuffer() != null, DLedgerResponseCode.DISK_ERROR, "Get null index for %d", index);
            indexSbr.getByteBuffer().getInt(); //magic
            long pos = indexSbr.getByteBuffer().getLong();
            int size = indexSbr.getByteBuffer().getInt();
            dataSbr = dataFileList.getData(pos, size);
            PreConditions.check(dataSbr != null && dataSbr.getByteBuffer() != null, DLedgerResponseCode.DISK_ERROR, "Get null data for %d", index);
            DLedgerEntry dLedgerEntry = DLedgerEntryCoder.decode(dataSbr.getByteBuffer());
            PreConditions.check(pos == dLedgerEntry.getPos(), DLedgerResponseCode.DISK_ERROR, "%d != %d", pos, dLedgerEntry.getPos());
            return dLedgerEntry;
        } finally {
            SelectMmapBufferResult.release(indexSbr);
            SelectMmapBufferResult.release(dataSbr);
        }
    }

    public Pair<Long, Integer> getEntryPosAndSize(Long index) {
        indexCheck(index);
        SelectMmapBufferResult indexSbr = null;
        try {
            indexSbr = indexFileList.getData(index * INDEX_UNIT_SIZE, INDEX_UNIT_SIZE);
            PreConditions.check(indexSbr != null && indexSbr.getByteBuffer() != null, DLedgerResponseCode.DISK_ERROR, "Get null index for %d", index);
            indexSbr.getByteBuffer().getInt(); //magic
            long pos = indexSbr.getByteBuffer().getLong();
            int size = indexSbr.getByteBuffer().getInt();
            return new Pair<>(pos, size);
        } finally {
            SelectMmapBufferResult.release(indexSbr);
        }
    }
    public void indexCheck(Long index) {
        PreConditions.check(index >= 0, DLedgerResponseCode.INDEX_OUT_OF_RANGE, "%d should gt 0", index);
        PreConditions.check(index >= ledgerBeginIndex, DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN, "%d should be gt %d, ledgerBeginIndex may be revised", index, ledgerBeginIndex);
        PreConditions.check(index <= ledgerEndIndex, DLedgerResponseCode.INDEX_OUT_OF_RANGE, "%d should between %d-%d", index, ledgerBeginIndex, ledgerEndIndex);
    }

    @Override
    public long getCommittedIndex() {
        return committedIndex;
    }

    @Override
    public void updateCommittedIndex(long term, long newCommittedIndex) {
        if (newCommittedIndex == -1
            || ledgerEndIndex == -1
            || term < memberState.currTerm()
            || newCommittedIndex == this.committedIndex) {
            return;
        }
        if (newCommittedIndex < this.committedIndex
            || newCommittedIndex < this.ledgerBeginIndex) {
            logger.warn("[MONITOR]Skip update committed index for new={} < old={} or new={} < beginIndex={}", newCommittedIndex, this.committedIndex, newCommittedIndex, this.ledgerBeginIndex);
            return;
        }
        long endIndex = ledgerEndIndex;
        if (newCommittedIndex > endIndex) {
            //If the node fall behind too much, the committedIndex will be larger than enIndex.
            newCommittedIndex = endIndex;
        }
        Pair<Long, Integer> posAndSize = getEntryPosAndSize(newCommittedIndex);
        PreConditions.check(posAndSize != null, DLedgerResponseCode.DISK_ERROR);
        this.committedIndex = newCommittedIndex;
        this.committedPos = posAndSize.getKey() + posAndSize.getValue();
    }

    @Override
    public long getLedgerEndTerm() {
        return ledgerEndTerm;
    }

    public long getCommittedPos() {
        return committedPos;
    }

    public void addAppendHook(AppendHook writeHook) {
        if (!appendHooks.contains(writeHook)) {
            appendHooks.add(writeHook);
        }
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

    public Set<String> getFullStorePaths() {
        return fullStorePaths;
    }

    public void setFullStorePaths(Set<String> fullStorePaths) {
        this.fullStorePaths = fullStorePaths;
    }

    public interface AppendHook {
        void doHook(DLedgerEntry entry, ByteBuffer buffer, int bodyOffset);
    }

    // Just for test
    public void shutdownFlushService() {
        this.flushDataService.shutdown();
    }

    class FlushDataService extends ShutdownAbleThread {

        public FlushDataService(String name, Logger logger) {
            super(name, logger);
        }

        @Override public void doWork() {
            try {
                long start = System.currentTimeMillis();
                DLedgerMmapFileStore.this.dataFileList.flush(0);
                DLedgerMmapFileStore.this.indexFileList.flush(0);
                long elapsed;
                if ((elapsed = DLedgerUtils.elapsed(start)) > 500) {
                    logger.info("Flush data cost={} ms", elapsed);
                }

                if (DLedgerUtils.elapsed(lastCheckPointTimeMs) > dLedgerConfig.getCheckPointInterval()) {
                    persistCheckPoint();
                    lastCheckPointTimeMs = System.currentTimeMillis();
                }

                waitForRunning(dLedgerConfig.getFlushFileInterval());
            } catch (Throwable t) {
                logger.info("Error in {}", getName(), t);
                DLedgerUtils.sleep(200);
            }
        }
    }

    class CleanSpaceService extends ShutdownAbleThread {

        double storeBaseRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getStoreBaseDir());
        double dataRatio = calcDataStorePathPhysicRatio();

        public CleanSpaceService(String name, Logger logger) {
            super(name, logger);
        }

        @Override public void doWork() {
            try {
                storeBaseRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getStoreBaseDir());
                dataRatio = calcDataStorePathPhysicRatio();
                long hourOfMs = 3600L * 1000L;
                long fileReservedTimeMs = dLedgerConfig.getFileReservedHours() *  hourOfMs;
                if (fileReservedTimeMs < hourOfMs) {
                    logger.warn("The fileReservedTimeMs={} is smaller than hourOfMs={}", fileReservedTimeMs, hourOfMs);
                    fileReservedTimeMs =  hourOfMs;
                }
                //If the disk is full, should prevent more data to get in
                DLedgerMmapFileStore.this.isDiskFull = isNeedForbiddenWrite();
                boolean timeUp = isTimeToDelete();
                boolean checkExpired = isNeedCheckExpired();
                boolean forceClean = isNeedForceClean();
                boolean enableForceClean = dLedgerConfig.isEnableDiskForceClean();
                int intervalForcibly = 120 * 1000;
                if (timeUp || checkExpired) {
                    int count = getDataFileList().deleteExpiredFileByTime(fileReservedTimeMs, 100, intervalForcibly, forceClean && enableForceClean);
                    if (count > 0 || (forceClean && enableForceClean) || isDiskFull) {
                        logger.info("Clean space count={} timeUp={} checkExpired={} forceClean={} enableForceClean={} diskFull={} storeBaseRatio={} dataRatio={}",
                            count, timeUp, checkExpired, forceClean, enableForceClean, isDiskFull, storeBaseRatio, dataRatio);
                    }
                    if (count > 0) {
                        DLedgerMmapFileStore.this.reviseLedgerBeginIndex();
                    }
                }
                getDataFileList().retryDeleteFirstFile(intervalForcibly);
                waitForRunning(100);
            } catch (Throwable t) {
                logger.info("Error in {}", getName(), t);
                DLedgerUtils.sleep(200);
            }
        }

        private boolean isTimeToDelete() {
            String when = DLedgerMmapFileStore.this.dLedgerConfig.getDeleteWhen();
            if (DLedgerUtils.isItTimeToDo(when)) {
                return true;
            }

            return false;
        }

        private boolean isNeedCheckExpired() {
            if (storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired()
                || dataRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired()) {
                return true;
            }
            return false;
        }

        private boolean isNeedForceClean() {
            if (storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()
                || dataRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()) {
                return true;
            }
            return false;
        }

        private boolean isNeedForbiddenWrite() {
            if (storeBaseRatio > dLedgerConfig.getDiskFullRatio()
                || dataRatio > dLedgerConfig.getDiskFullRatio()) {
                return true;
            }
            return false;
        }

        public double calcDataStorePathPhysicRatio() {
            Set<String> fullStorePath = new HashSet<>();
            String storePath = dLedgerConfig.getDataStorePath();
            String[] paths = storePath.trim().split(DLedgerConfig.MULTI_PATH_SPLITTER);
            double minPhysicRatio = 100;
            for (String path : paths) {
                double physicRatio = DLedgerUtils.isPathExists(path) ? DLedgerUtils.getDiskPartitionSpaceUsedPercent(path) : -1;
                minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
                if (physicRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()) {
                    fullStorePath.add(path);
                }
            }
            DLedgerMmapFileStore.this.setFullStorePaths(fullStorePath);
            return minPhysicRatio;
        }
    }
}
