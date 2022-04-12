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

import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MmapFileList {
    public static final int MIN_BLANK_LEN = 8;
    public static final int BLANK_MAGIC_CODE = -1;
    private static Logger logger = LoggerFactory.getLogger(MmapFile.class);
    private static final int DELETE_FILES_BATCH_MAX = 10;
    private final String storePath;

    private final int mappedFileSize;

    private final CopyOnWriteArrayList<MmapFile> mappedFiles = new CopyOnWriteArrayList<MmapFile>();

    private long flushedWhere = 0;
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    public MmapFileList(final String storePath, int mappedFileSize) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
    }

    public boolean checkSelf() {
        if (!this.mappedFiles.isEmpty()) {
            Iterator<MmapFile> iterator = mappedFiles.iterator();
            MmapFile pre = null;
            while (iterator.hasNext()) {
                MmapFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        logger.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                        return false;
                    }
                }
                pre = cur;
            }
        }
        return true;
    }

    public MmapFile getMappedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMappedFiles();

        if (null == mfs) {
            return null;
        }

        for (int i = 0; i < mfs.length; i++) {
            MmapFile mappedFile = (MmapFile) mfs[i];
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MmapFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles() {
        if (this.mappedFiles.size() <= 0) {
            return null;
        }
        return this.mappedFiles.toArray();
    }

    public void truncateOffset(long offset) {
        Object[] mfs = this.copyMappedFiles();
        if (mfs == null) {
            return;
        }
        List<MmapFile> willRemoveFiles = new ArrayList<MmapFile>();

        for (int i = 0; i < mfs.length; i++) {
            MmapFile file = (MmapFile) mfs[i];
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    willRemoveFiles.add(file);
                }
            }
        }

        this.destroyExpiredFiles(willRemoveFiles);
        this.deleteExpiredFiles(willRemoveFiles);
    }

    void destroyExpiredFiles(List<MmapFile> files) {
        Collections.sort(files, (o1, o2) -> {
            if (o1.getFileFromOffset() < o2.getFileFromOffset()) {
                return -1;
            } else if (o1.getFileFromOffset() > o2.getFileFromOffset()) {
                return 1;
            }
            return 0;
        });

        for (int i = 0; i < files.size(); i++) {
            MmapFile mmapFile = files.get(i);
            while (true) {
                if (mmapFile.destroy(10 * 1000)) {
                    break;
                }
                DLedgerUtils.sleep(1000);
            }
        }
    }

    public void resetOffset(long offset) {
        Object[] mfs = this.copyMappedFiles();
        if (mfs == null) {
            return;
        }
        List<MmapFile> willRemoveFiles = new ArrayList<MmapFile>();

        for (int i = mfs.length - 1; i >= 0; i--) {
            MmapFile file = (MmapFile) mfs[i];
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (file.getFileFromOffset() <= offset) {
                if (offset < fileTailOffset) {
                    file.setStartPosition((int) (offset % this.mappedFileSize));
                } else {
                    willRemoveFiles.add(file);
                }
            }
        }

        this.destroyExpiredFiles(willRemoveFiles);
        this.deleteExpiredFiles(willRemoveFiles);
    }

    public void updateWherePosition(long wherePosition) {
        if (wherePosition > getMaxWrotePosition()) {
            logger.warn("[UpdateWherePosition] wherePosition {} > maxWrotePosition {}", wherePosition, getMaxWrotePosition());
            return;
        }
        this.setFlushedWhere(wherePosition);
        this.setCommittedWhere(wherePosition);
    }

    public long append(byte[] data) {
        return append(data, 0, data.length);
    }

    public long append(byte[] data, int pos, int len) {
        return append(data, pos, len, true);
    }

    public long append(byte[] data, boolean useBlank) {
        return append(data, 0, data.length, useBlank);
    }

    public long preAppend(int len) {
        return preAppend(len, true);
    }

    public long preAppend(int len, boolean useBlank) {
        MmapFile mappedFile = getLastMappedFile();
        if (null == mappedFile || mappedFile.isFull()) {
            mappedFile = getLastMappedFile(0);
        }
        if (null == mappedFile) {
            logger.error("Create mapped file for {}", storePath);
            return -1;
        }
        int blank = useBlank ? MIN_BLANK_LEN : 0;
        if (len + blank > mappedFile.getFileSize() - mappedFile.getWrotePosition()) {
            if (blank < MIN_BLANK_LEN) {
                logger.error("Blank {} should ge {}", blank, MIN_BLANK_LEN);
                return -1;
            } else {
                ByteBuffer byteBuffer = ByteBuffer.allocate(mappedFile.getFileSize() - mappedFile.getWrotePosition());
                byteBuffer.putInt(BLANK_MAGIC_CODE);
                byteBuffer.putInt(mappedFile.getFileSize() - mappedFile.getWrotePosition());
                if (mappedFile.appendMessage(byteBuffer.array())) {
                    //need to set the wrote position
                    mappedFile.setWrotePosition(mappedFile.getFileSize());
                } else {
                    logger.error("Append blank error for {}", storePath);
                    return -1;
                }
                mappedFile = getLastMappedFile(0);
                if (null == mappedFile) {
                    logger.error("Create mapped file for {}", storePath);
                    return -1;
                }
            }
        }
        return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();

    }

    public long append(byte[] data, int pos, int len, boolean useBlank) {
        if (preAppend(len, useBlank) == -1) {
            return -1;
        }
        MmapFile mappedFile = getLastMappedFile();
        long currPosition = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        if (!mappedFile.appendMessage(data, pos, len)) {
            logger.error("Append error for {}", storePath);
            return -1;
        }
        return currPosition;
    }

    public SelectMmapBufferResult getData(final long offset, final int size) {
        MmapFile mappedFile = findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public SelectMmapBufferResult getData(final long offset) {
        MmapFile mappedFile = findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    void deleteExpiredFiles(List<MmapFile> files) {

        if (!files.isEmpty()) {

            Iterator<MmapFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MmapFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    logger.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    logger.error("deleteExpiredFiles remove failed.");
                }
            } catch (Exception e) {
                logger.error("deleteExpiredFiles has exception.", e);
            }
        }
    }

    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            return doLoad(Arrays.asList(files));
        }
        return true;
    }

    public boolean doLoad(List<File> files) {
        // ascending order
        files.sort(Comparator.comparing(File::getName));
        for (File file : files) {

            if (file.length() != this.mappedFileSize) {
                logger.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually. You should delete old files before changing mapped file size");
                return false;
            }
            try {
                MmapFile mappedFile = new DefaultMmapFile(file.getPath(), mappedFileSize);

                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                logger.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                logger.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public MmapFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MmapFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        } else if (mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    protected MmapFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + DLedgerUtils.offset2FileName(createOffset);
        return doCreateMappedFile(nextFilePath);
    }

    protected MmapFile doCreateMappedFile(String nextFilePath) {
        MmapFile mappedFile = null;
        try {
            mappedFile = new DefaultMmapFile(nextFilePath, this.mappedFileSize);
        } catch (IOException e) {
            logger.error("create mappedFile exception", e);
        }

        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    public MmapFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MmapFile getLastMappedFile() {
        MmapFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                logger.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public long getMinOffset() {
        MmapFile mmapFile = getFirstMappedFile();
        if (mmapFile != null) {
            return mmapFile.getFileFromOffset() + mmapFile.getStartPosition();
        }
        return 0;
    }

    public long getMaxReadPosition() {
        MmapFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MmapFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxReadPosition() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MmapFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            logger.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles();

        if (null == mfs) {
            return 0;
        }

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MmapFile> files = new ArrayList<MmapFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MmapFile mappedFile = (MmapFile) mfs[i];
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        files.add(mappedFile);
                        deleteCount++;

                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFiles(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles();

        List<MmapFile> files = new ArrayList<MmapFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MmapFile mappedFile = (MmapFile) mfs[i];
                SelectMmapBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        logger.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    logger.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    logger.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFiles(files);

        return deleteCount;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MmapFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
        }

        return result;
    }

    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MmapFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * Finds a mapped file by offset.
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MmapFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MmapFile firstMappedFile = this.getFirstMappedFile();
            MmapFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    logger.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MmapFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    logger.warn("Offset is matched, but get file failed, maybe the file number is changed. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());

                    for (MmapFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            logger.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MmapFile getFirstMappedFile() {
        MmapFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                logger.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MmapFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles();
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MmapFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                logger.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    logger.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MmapFile> tmpFiles = new ArrayList<MmapFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFiles(tmpFiles);
                } else {
                    logger.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MmapFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MmapFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public boolean rebuildWithPos(long pos) {
        truncateOffset(-1);
        getLastMappedFile(pos);
        truncateOffset(pos);
        resetOffset(pos);
        return pos == getMaxWrotePosition() && pos == getMinOffset();
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MmapFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
