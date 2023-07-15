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
import io.openmessaging.storage.dledger.common.ShutdownAbleThread;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.entry.DLedgerEntryCoder;
import io.openmessaging.storage.dledger.entry.DLedgerEntryType;
import io.openmessaging.storage.dledger.entry.DLedgerIndexEntry;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerMmapFileStore extends DLedgerStore {
    public static final int INDEX_UNIT_SIZE = 32;

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerMmapFileStore.class);
    public List<AppendHook> appendHooks = new ArrayList<>();

    private volatile long ledgerBeforeBeginIndex = -1;

    private volatile long ledgerBeforeBeginTerm = -1;

    private long ledgerEndIndex = -1;
    private long ledgerEndTerm;
    private final DLedgerConfig dLedgerConfig;
    private final MemberState memberState;
    private final MmapFileList dataFileList;
    private final MmapFileList indexFileList;
    private final ThreadLocal<ByteBuffer> localEntryBuffer;
    private final ThreadLocal<ByteBuffer> localIndexBuffer;
    private final FlushDataService flushDataService;
    private final CleanSpaceService cleanSpaceService;
    private volatile boolean isDiskFull = false;

    private long lastCheckPointTimeMs = System.currentTimeMillis();

    private final AtomicBoolean hasLoaded = new AtomicBoolean(false);
    private final AtomicBoolean hasRecovered = new AtomicBoolean(false);

    private volatile Set<String> fullStorePaths = Collections.emptySet();

    private boolean enableCleanSpaceService = true;

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
        flushDataService = new FlushDataService("DLedgerFlushDataService", LOGGER);
        cleanSpaceService = new CleanSpaceService("DLedgerCleanSpaceService", LOGGER);
    }

    @Override
    public void startup() {
        load();
        recover();
        flushDataService.start();
        if (enableCleanSpaceService) {
            cleanSpaceService.start();
        }
    }

    @Override
    public void shutdown() {
        this.dataFileList.flush(0);
        this.indexFileList.flush(0);
        if (enableCleanSpaceService) {
            cleanSpaceService.shutdown();
        }
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
            LOGGER.error("Load file failed, this usually indicates fatal error, you should check it manually");
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
                if (magic == MmapFileList.BLANK_MAGIC_CODE) {
                    LOGGER.info("Find blank magic code at the file: {}", mappedFile.getFileName());
                    continue;
                }
                int size = byteBuffer.getInt();
                long entryIndex = byteBuffer.getLong();
                long entryTerm = byteBuffer.getLong();
                byteBuffer.getLong();
                byteBuffer.getInt(); //channel
                byteBuffer.getInt(); //chain crc
                byteBuffer.getInt(); //body crc
                int bodySize = byteBuffer.getInt();
                PreConditions.check(magic != MmapFileList.BLANK_MAGIC_CODE && DLedgerEntryType.isValid(magic), DLedgerResponseCode.DISK_ERROR, "unknown magic=%d", magic);
                PreConditions.check(size > DLedgerEntry.HEADER_SIZE, DLedgerResponseCode.DISK_ERROR, "Size %d should > %d", size, DLedgerEntry.HEADER_SIZE);

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
                LOGGER.warn("Pre check data and index failed {}", mappedFile.getFileName(), t);
            }
        }

        MmapFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        LOGGER.info("Begin to recover data from entryIndex={} fileIndex={} fileSize={} fileName={} ", firstEntryIndex, index, mappedFiles.size(), mappedFile.getFileName());
        long lastEntryIndex = -1;
        long lastEntryTerm = -1;
        long processOffset = mappedFile.getFileFromOffset();
        boolean needWriteIndex = false;
        while (true) {
            try {
                int relativePos = byteBuffer.position();
                long absolutePos = mappedFile.getFileFromOffset() + relativePos;
                int magic = byteBuffer.getInt();
                int size = byteBuffer.getInt();
                if (magic == MmapFileList.BLANK_MAGIC_CODE) {
                    processOffset += size;
                    if (relativePos + size == mappedFile.getFileSize()) {
                        // next file
                        index++;
                        if (index >= mappedFiles.size()) {
                            LOGGER.info("Recover data file over, the last file {}", mappedFile.getFileName());
                            break;
                        }
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        LOGGER.info("Trying to recover data file {}", mappedFile.getFileName());
                        continue;
                    }
                    byteBuffer.position(relativePos + size);
                    continue;
                }
                if (size == 0) {
                    LOGGER.info("Recover data file to the end of {} ", mappedFile.getFileName());
                    break;
                }
                long entryIndex = byteBuffer.getLong();
                long entryTerm = byteBuffer.getLong();
                byteBuffer.getLong(); // position
                byteBuffer.getInt(); // channel
                byteBuffer.getInt(); // chain crc
                byteBuffer.getInt(); // body crc
                int bodySize = byteBuffer.getInt();

                PreConditions.check(bodySize + DLedgerEntry.BODY_OFFSET == size, DLedgerResponseCode.DISK_ERROR, "size %d != %d + %d", size, bodySize, DLedgerEntry.BODY_OFFSET);

                byteBuffer.position(relativePos + size);

                PreConditions.check(DLedgerEntryType.isValid(magic), DLedgerResponseCode.DISK_ERROR, "pos=%d size=%d magic=%d index=%d term=%d", absolutePos, size, magic, entryIndex, entryTerm);
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
                        LOGGER.warn("Compare data to index failed {}", mappedFile.getFileName(), t);
                        indexFileList.truncateOffset(entryIndex * INDEX_UNIT_SIZE);
                        if (indexFileList.getMaxWrotePosition() != entryIndex * INDEX_UNIT_SIZE) {
                            long truncateIndexOffset = entryIndex * INDEX_UNIT_SIZE;
                            LOGGER.warn("[Recovery] rebuild for index wrotePos={} not equal to truncatePos={}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
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
                LOGGER.info("Recover data file to the end of {} ", mappedFile.getFileName(), t);
                break;
            }
        }
        LOGGER.info("Recover data to the end entryIndex={} processOffset={} lastFileOffset={} cha={}",
            lastEntryIndex, processOffset, lastMappedFile.getFileFromOffset(), processOffset - lastMappedFile.getFileFromOffset());
        if (lastMappedFile.getFileFromOffset() - processOffset > lastMappedFile.getFileSize()) {
            LOGGER.error("[MONITOR]The processOffset is too small, you should check it manually before truncating the data from {}", processOffset);
            System.exit(-1);
        }

        ledgerEndIndex = lastEntryIndex;
        ledgerEndTerm = lastEntryTerm;
        if (lastEntryIndex != -1) {
            DLedgerEntry entry = get(lastEntryIndex);
            PreConditions.check(entry != null, DLedgerResponseCode.DISK_ERROR, "recheck get null entry");
            PreConditions.check(entry.getIndex() == lastEntryIndex, DLedgerResponseCode.DISK_ERROR, "recheck index %d != %d", entry.getIndex(), lastEntryIndex);
            reviseLedgerBeforeBeginIndex();
        }
        this.dataFileList.updateWherePosition(processOffset);
        this.dataFileList.truncateOffset(processOffset);
        long indexProcessOffset = (lastEntryIndex + 1) * INDEX_UNIT_SIZE;
        this.indexFileList.updateWherePosition(indexProcessOffset);
        this.indexFileList.truncateOffset(indexProcessOffset);
        updateLedgerEndIndexAndTerm();
        PreConditions.check(dataFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check data file order failed after recovery");
        PreConditions.check(indexFileList.checkSelf(), DLedgerResponseCode.DISK_ERROR, "check index file order failed after recovery");

    }

    private void reviseLedgerBeforeBeginIndex() {
        // get ledger begin index
        dataFileList.checkFirstFileAllBlank();
        MmapFile firstFile = dataFileList.getFirstMappedFile();
        SelectMmapBufferResult sbr = firstFile.selectMappedBuffer(0);
        try {
            ByteBuffer tmpBuffer = sbr.getByteBuffer();
            tmpBuffer.position(firstFile.getStartPosition());
            int magic = tmpBuffer.getInt();//magic
            int size = tmpBuffer.getInt();//size
            if (magic == MmapFileList.BLANK_MAGIC_CODE) {
                tmpBuffer.position(firstFile.getStartPosition() + size);
                tmpBuffer.getInt();
                size = tmpBuffer.getInt();
            }
            if (size == 0) {
                // means that now empty entry
                return;
            }
            // begin index
            long beginIndex = tmpBuffer.getLong();
            this.ledgerBeforeBeginIndex = beginIndex - 1;
            indexFileList.resetOffset(beginIndex * INDEX_UNIT_SIZE);
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
            DLedgerEntryCoder.setIndexTerm(dataBuffer, nextIndex, memberState.currTerm(), entry.getMagic());
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
            DLedgerEntryCoder.encodeIndex(dataPos, entrySize, DLedgerEntryType.NORMAL.getMagic(), nextIndex, memberState.currTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.info("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            ledgerEndIndex++;
            ledgerEndTerm = memberState.currTerm();
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
                LOGGER.warn("[TRUNCATE]leaderId={} index={} truncatePos={} != maxPos={}, this is usually happened on the old leader", leaderId, entry.getIndex(), truncatePos, dataFileList.getMaxWrotePosition());
            }
            dataFileList.truncateOffset(truncatePos);
            if (dataFileList.getMaxWrotePosition() != truncatePos) {
                LOGGER.warn("[TRUNCATE] rebuild for data wrotePos: {} != truncatePos: {}", dataFileList.getMaxWrotePosition(), truncatePos);
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
                LOGGER.warn("[TRUNCATE] rebuild for index wrotePos: {} != truncatePos: {}", indexFileList.getMaxWrotePosition(), truncateIndexOffset);
                PreConditions.check(indexFileList.rebuildWithPos(truncateIndexOffset), DLedgerResponseCode.DISK_ERROR, "rebuild index truncatePos=%d", truncateIndexOffset);
            }
            reviseIndexFileListFlushedWhere(truncateIndexOffset);
            DLedgerEntryCoder.encodeIndex(entry.getPos(), entrySize, entry.getMagic(), entry.getIndex(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            ledgerEndTerm = entry.getTerm();
            ledgerEndIndex = entry.getIndex();
            reviseLedgerBeforeBeginIndex();
            updateLedgerEndIndexAndTerm();
            return entry.getIndex();
        }
    }

    @Override
    public long truncate(long truncateIndex) {
        if (truncateIndex > this.ledgerEndIndex) {
            return this.ledgerEndIndex;
        }
        DLedgerEntry firstTruncateEntry = this.get(truncateIndex);
        long truncateStartPos = firstTruncateEntry.getPos();
        synchronized (this.memberState) {
            if (truncateIndex > this.ledgerEndIndex) {
                return this.ledgerEndIndex;
            }
            // truncate data file
            dataFileList.truncateOffset(truncateStartPos);
            if (dataFileList.getMaxWrotePosition() != truncateStartPos) {
                LOGGER.warn("[TRUNCATE] truncate for data file error, try to truncate pos: {}, but after truncate, max wrote pos: {}, now try to rebuild", truncateStartPos, dataFileList.getMaxWrotePosition());
                PreConditions.check(dataFileList.rebuildWithPos(truncateStartPos), DLedgerResponseCode.DISK_ERROR, "rebuild data file truncatePos=%d", truncateStartPos);
            }
            reviseDataFileListFlushedWhere(truncateStartPos);

            // truncate index file
            long truncateIndexFilePos = truncateIndex * INDEX_UNIT_SIZE;
            indexFileList.truncateOffset(truncateIndexFilePos);
            if (indexFileList.getMaxWrotePosition() != truncateIndexFilePos) {
                LOGGER.warn("[TRUNCATE] truncate for index file error, try to truncate pos: {}, but after truncate, max wrote pos: {}, now try to rebuild", truncateIndexFilePos, indexFileList.getMaxWrotePosition());
                PreConditions.check(dataFileList.rebuildWithPos(truncateStartPos), DLedgerResponseCode.DISK_ERROR, "rebuild index file truncatePos=%d", truncateIndexFilePos);
            }
            reviseIndexFileListFlushedWhere(truncateIndexFilePos);

            // update store end index and its term
            if (truncateIndex == 0) {
                // now clear all entries
                ledgerEndTerm = -1;
                ledgerEndIndex = -1;
            } else {
                SelectMmapBufferResult endIndexBuf = indexFileList.getData((truncateIndex - 1) * INDEX_UNIT_SIZE, INDEX_UNIT_SIZE);
                ByteBuffer buffer = endIndexBuf.getByteBuffer();
                DLedgerIndexEntry indexEntry = DLedgerEntryCoder.decodeIndex(buffer);
                ledgerEndTerm = indexEntry.getTerm();
                ledgerEndIndex = indexEntry.getIndex();
            }
        }
        LOGGER.info("[TRUNCATE] truncateIndex: {}, after truncate, ledgerEndIndex: {} ledgerEndTerm: {}", truncateIndex, ledgerEndIndex, ledgerEndTerm);
        return ledgerEndIndex;
    }

    @Override
    public long reset(long beforeBeginIndex, long beforeBeginTerm) {
        // clear all entries in [.., beforeBeginIndex]
        if (beforeBeginIndex <= this.ledgerBeforeBeginIndex) {
            return this.ledgerBeforeBeginIndex + 1;
        }
        synchronized (this.memberState) {
            if (beforeBeginIndex <= this.ledgerBeforeBeginIndex) {
                return this.ledgerBeforeBeginIndex + 1;
            }
            if (beforeBeginIndex >= this.ledgerEndIndex) {
                // after reset, we should have empty entries
                SelectMmapBufferResult endIndexResult = indexFileList.getData(this.ledgerEndIndex * INDEX_UNIT_SIZE);
                if (endIndexResult != null) {
                    DLedgerIndexEntry resetEntry = DLedgerEntryCoder.decodeIndex(endIndexResult.getByteBuffer());
                    this.dataFileList.resetOffset(resetEntry.getPosition() + resetEntry.getSize());
                    endIndexResult.release();
                }
                this.indexFileList.rebuildWithPos((beforeBeginIndex + 1) * INDEX_UNIT_SIZE);
            } else {
                SelectMmapBufferResult data = indexFileList.getData((beforeBeginIndex + 1) * INDEX_UNIT_SIZE);
                DLedgerIndexEntry resetEntry = DLedgerEntryCoder.decodeIndex(data.getByteBuffer());
                data.release();
                this.dataFileList.resetOffset(resetEntry.getPosition());
                this.indexFileList.resetOffset((beforeBeginIndex + 1) * INDEX_UNIT_SIZE);
            }
            this.ledgerBeforeBeginIndex = beforeBeginIndex;
            this.ledgerBeforeBeginTerm = beforeBeginTerm;
            if (beforeBeginIndex >= this.ledgerEndIndex) {
                this.ledgerEndIndex = beforeBeginIndex;
                this.ledgerEndTerm = beforeBeginTerm;
            }
        }
        LOGGER.info("reset to beforeBeginIndex: {}, beforeBeginTerm: {}, now beforeBeginIndex: {}, beforeBeginTerm: {}, endIndex: {}, endTerm: {}",
            beforeBeginIndex, beforeBeginTerm, ledgerBeforeBeginIndex, ledgerBeforeBeginTerm, ledgerEndIndex, ledgerEndTerm);
        return ledgerBeforeBeginIndex + 1;
    }

    private void reviseDataFileListFlushedWhere(long truncatePos) {
        long offset = calculateWherePosition(this.dataFileList, truncatePos);
        LOGGER.info("Revise dataFileList flushedWhere from {} to {}", this.dataFileList.getFlushedWhere(), offset);
        // It seems unnecessary to set position atomically. Wrong position won't get updated during flush or commit.
        this.dataFileList.updateWherePosition(offset);
    }

    private void reviseIndexFileListFlushedWhere(long truncateIndexOffset) {
        long offset = calculateWherePosition(this.indexFileList, truncateIndexOffset);
        LOGGER.info("Revise indexFileList flushedWhere from {} to {}", this.indexFileList.getFlushedWhere(), offset);
        this.indexFileList.updateWherePosition(offset);
    }

    /**
     * calculate wherePosition after truncate
     *
     * @param mappedFileList       this.dataFileList or this.indexFileList
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
        return Math.min(mappedFileList.getFlushedWhere(), continuedBeginOffset);
    }

    @Override
    public void resetOffsetAfterSnapshot(DLedgerEntry entry) {
        // judge expired
        if (entry.getIndex() <= this.ledgerBeforeBeginIndex) {
            return;
        }
        synchronized (this.memberState) {
            long resetPos = entry.getPos() + entry.getSize();
            dataFileList.resetOffset(resetPos);
            long resetIndexOffset = entry.getIndex() * INDEX_UNIT_SIZE;
            indexFileList.resetOffset(resetIndexOffset);
            // reset ledgerBeforeBeginIndex
            this.ledgerBeforeBeginIndex = entry.getIndex();
        }
    }

    @Override
    public void updateIndexAfterLoadingSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        this.ledgerBeforeBeginIndex = lastIncludedIndex;
        this.ledgerEndIndex = lastIncludedIndex;
        this.ledgerEndTerm = lastIncludedTerm;
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
            DLedgerEntryCoder.encodeIndex(dataPos, entrySize, entry.getMagic(), entry.getIndex(), entry.getTerm(), indexBuffer);
            long indexPos = indexFileList.append(indexBuffer.array(), 0, indexBuffer.remaining(), false);
            PreConditions.check(indexPos == entry.getIndex() * INDEX_UNIT_SIZE, DLedgerResponseCode.DISK_ERROR, null);
            ledgerEndTerm = entry.getTerm();
            ledgerEndIndex = entry.getIndex();
            updateLedgerEndIndexAndTerm();
            return entry;
        }

    }

    @Override
    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    @Override
    public long getLedgerBeforeBeginIndex() {
        return ledgerBeforeBeginIndex;
    }

    @Override
    public long getLedgerBeforeBeginTerm() {
        return ledgerBeforeBeginTerm;
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
            return dLedgerEntry;
        } finally {
            SelectMmapBufferResult.release(indexSbr);
            SelectMmapBufferResult.release(dataSbr);
        }
    }

    @Override
    public DLedgerEntry getFirstLogOfTargetTerm(long targetTerm, long endIndex) {
        DLedgerEntry entry = null;
        for (long i = endIndex; i > ledgerBeforeBeginIndex; i--) {
            DLedgerEntry currentEntry = get(i);
            if (currentEntry == null) {
                continue;
            }
            if (currentEntry.getTerm() == targetTerm) {
                entry = currentEntry;
                continue;
            }
            if (currentEntry.getTerm() < targetTerm) {
                break;
            }
        }
        return entry;
    }

    private Pair<Long, Integer> getEntryPosAndSize(Long index) {
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

    private void indexCheck(Long index) {
        PreConditions.check(index >= 0, DLedgerResponseCode.INDEX_OUT_OF_RANGE, "%d should gt 0", index);
        PreConditions.check(index > ledgerBeforeBeginIndex, DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN, "%d should be gt %d, beforeBeginIndex may be revised", index, ledgerBeforeBeginIndex);
        PreConditions.check(index <= ledgerEndIndex, DLedgerResponseCode.INDEX_OUT_OF_RANGE, "%d should between (%d-%d]", index, ledgerBeforeBeginIndex, ledgerEndIndex);
    }

    @Override
    public long getLedgerEndTerm() {
        return ledgerEndTerm;
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

    public void setEnableCleanSpaceService(boolean enableCleanSpaceService) {
        this.enableCleanSpaceService = enableCleanSpaceService;
    }

    class FlushDataService extends ShutdownAbleThread {

        public FlushDataService(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
            try {
                long start = System.currentTimeMillis();
                DLedgerMmapFileStore.this.dataFileList.flush(0);
                DLedgerMmapFileStore.this.indexFileList.flush(0);
                long elapsed;
                if ((elapsed = DLedgerUtils.elapsed(start)) > 500) {
                    logger.info("Flush data cost={} ms", elapsed);
                }

                if (DLedgerUtils.elapsed(lastCheckPointTimeMs) > dLedgerConfig.getCheckPointInterval()) {
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

        @Override
        public void doWork() {
            try {
                storeBaseRatio = DLedgerUtils.getDiskPartitionSpaceUsedPercent(dLedgerConfig.getStoreBaseDir());
                dataRatio = calcDataStorePathPhysicRatio();
                long hourOfMs = 3600L * 1000L;
                long fileReservedTimeMs = dLedgerConfig.getFileReservedHours() * hourOfMs;
                if (fileReservedTimeMs < hourOfMs) {
                    logger.warn("The fileReservedTimeMs={} is smaller than hourOfMs={}", fileReservedTimeMs, hourOfMs);
                    fileReservedTimeMs = hourOfMs;
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
                        DLedgerMmapFileStore.this.reviseLedgerBeforeBeginIndex();
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
            return DLedgerUtils.isItTimeToDo(when);
        }

        private boolean isNeedCheckExpired() {
            return storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired()
                || dataRatio > dLedgerConfig.getDiskSpaceRatioToCheckExpired();
        }

        private boolean isNeedForceClean() {
            return storeBaseRatio > dLedgerConfig.getDiskSpaceRatioToForceClean()
                || dataRatio > dLedgerConfig.getDiskSpaceRatioToForceClean();
        }

        private boolean isNeedForbiddenWrite() {
            return storeBaseRatio > dLedgerConfig.getDiskFullRatio()
                || dataRatio > dLedgerConfig.getDiskFullRatio();
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
