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
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultMmapFile extends ReferenceResource implements MmapFile {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static Logger logger = LoggerFactory.getLogger(DefaultMmapFile.class);
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    private static final AtomicIntegerFieldUpdater<DefaultMmapFile> START_POSITION_UPDATER;
    private static final AtomicIntegerFieldUpdater<DefaultMmapFile> WROTE_POSITION_UPDATER;
    private static final AtomicIntegerFieldUpdater<DefaultMmapFile> COMMITTED_POSITION_UPDATER;
    private static final AtomicIntegerFieldUpdater<DefaultMmapFile> FLUSHED_POSITION_UPDATER;

    static {
        START_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMmapFile.class, "startPosition");
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMmapFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMmapFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMmapFile.class, "flushedPosition");
    }

    private volatile int startPosition = 0;
    private volatile int wrotePosition = 0;
    private volatile int committedPosition = 0;
    private volatile int flushedPosition = 0;

    protected File file;
    int fileSize;
    long fileFromOffset;
    private FileChannel fileChannel;
    private final String fileName;
    private final MappedByteBuffer mappedByteBuffer;
    private volatile long storeTimestamp = 0;
    private boolean firstCreateInQueue = false;

    public DefaultMmapFile(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            logger.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            logger.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                logger.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }
        if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
            invokeAfterJava8(buffer);
        } else {
            invoke(invoke(viewed(buffer), "cleaner"), "clean");
        }
    }
    
    private static void invokeAfterJava8(final ByteBuffer buffer) {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Unsafe unsafe = (Unsafe) field.get(null);
            Method cleaner = method(unsafe, "invokeCleaner", new Class[] {ByteBuffer.class});
            cleaner.invoke(unsafe, viewed(buffer));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("buffer is non-direct");
        }
        
        ByteBuffer viewedBuffer = (ByteBuffer) ((DirectBuffer) buffer).attachment();
        if (viewedBuffer == null) {
            return buffer;
        } else {
            return viewed(viewedBuffer);
        }
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition;

        if ((currentPos + length) <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data, offset, length);
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }
        return false;
    }

    /**
     * @return The current flushed position
     */
    @Override
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();
                try {
                    this.mappedByteBuffer.force();
                } catch (Throwable e) {
                    logger.error("Error occurred when force data to disk.", e);
                }

                FLUSHED_POSITION_UPDATER.set(this, value);
                this.release();
            } else {
                logger.warn("in flush, hold failed, flush offset = " + this.flushedPosition);
                FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    @Override
    public int commit(final int commitLeastPages) {
        COMMITTED_POSITION_UPDATER.set(this, this.wrotePosition);
        return this.committedPosition;
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flushedPos = this.flushedPosition;
        int writePos = getReadPosition();

        if (this.isFull()) {
            return writePos > flushedPos;
        }

        if (flushLeastPages > 0) {
            return ((writePos / OS_PAGE_SIZE) - (flushedPos / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return writePos > flushedPos;
    }

    @Override
    public int getFlushedPosition() {
        return this.flushedPosition;
    }

    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public int getStartPosition() {
        return this.startPosition;
    }

    @Override
    public void setStartPosition(int startPosition) {
        START_POSITION_UPDATER.set(this, startPosition);
    }

    @Override
    public boolean isFull() {
        return this.fileSize == this.wrotePosition;
    }

    @Override
    public SelectMmapBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMmapBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                logger.warn("matched, but hold failed, request pos={} fileFromOffset={}", pos, this.fileFromOffset);
            }
        } else {
            logger.warn("selectMappedBuffer request pos invalid, request pos={} size={} fileFromOffset={} readPos={}", pos, size, fileFromOffset, readPosition);
        }

        return null;
    }

    @Override
    public SelectMmapBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMmapBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }

        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    logger.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                logger.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            logger.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return false;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            logger.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            logger.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        logger.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    @Override
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                logger.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                logger.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + DLedgerUtils.computeEclipseTimeMilliseconds(beginTime));
                Thread.sleep(10);
            } catch (Exception e) {
                logger.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            logger.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    @Override
    public int getWrotePosition() {
        return this.wrotePosition;
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * @return The max position which have valid data
     */
    @Override
    public int getReadPosition() {
        return this.wrotePosition;
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
