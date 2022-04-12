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

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.ServerTestHarness;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.file.MmapFile;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.openmessaging.storage.dledger.store.file.MmapFileList.MIN_BLANK_LEN;

public class DLedgerMappedFileStoreTest extends ServerTestHarness {

    private synchronized DLedgerMmapFileStore createFileStore(String group, String peers, String selfId,
        String leaderId) {
        return createFileStore(group, peers, selfId, leaderId, 10 * 1024 * 1024, DLedgerMmapFileStore.INDEX_UNIT_SIZE * 1024 * 1024, 0);
    }

    private synchronized DLedgerMmapFileStore createFileStore(String group, String peers, String selfId, String leaderId,
        int dataFileSize, int indexFileSize, int deleteFileNums) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.MEMORY);
        config.setDiskSpaceRatioToForceClean(0.90f);
        config.setEnableDiskForceClean(false);
        config.setEnableLeaderElector(false);
        if (dataFileSize != -1) {
            config.setMappedFileSizeForEntryData(dataFileSize);
        }
        if (indexFileSize != -1) {
            config.setMappedFileSizeForEntryIndex(indexFileSize);
        }
        if (deleteFileNums > 0) {
            File dir = new File(config.getDataStorePath());
            File[] files = dir.listFiles();
            if (files != null) {
                Arrays.sort(files);
                for (int i = files.length - 1; i >= 0; i--) {
                    File file = files[i];
                    file.delete();
                    if (files.length - i >= deleteFileNums) {
                        break;
                    }
                }
            }
        }

        MemberState memberState = new MemberState(config);
        memberState.setCurrTermForTest(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        DLedgerMmapFileStore fileStore = new DLedgerMmapFileStore(config, memberState);
        fileStore.startup();
        return fileStore;
    }

    @Test
    public void testCommittedIndex() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerMmapFileStore fileStore = createFileStore(group,  peers, "n0", "n0");
        MemberState memberState = fileStore.getMemberState();
        for (int i = 0; i < 100; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody((new byte[128]));
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assertions.assertEquals(i, resEntry.getIndex());
        }
        fileStore.updateCommittedIndex(memberState.currTerm(), 90);
        Assertions.assertEquals(99, fileStore.getLedgerEndIndex());
        Assertions.assertEquals(90, fileStore.getCommittedIndex());

        while (fileStore.getFlushPos() != fileStore.getWritePos()) {
            fileStore.flush();
        }
        fileStore.shutdown();
        fileStore = createFileStore(group, peers, "n0", "n0");
        Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
        Assertions.assertEquals(99, fileStore.getLedgerEndIndex());
        Assertions.assertEquals(90, fileStore.getCommittedIndex());
    }

    @Test
    public void testAppendHook() throws Exception {
        String group = UUID.randomUUID().toString();
        DLedgerMmapFileStore fileStore = createFileStore(group, String.format("n0-localhost:%d", nextPort()), "n0", "n0");
        DLedgerMmapFileStore.AppendHook appendHook = (entry, buffer, bodyOffset) -> {
            buffer.position(bodyOffset);
            buffer.putLong(entry.getIndex());
        };
        fileStore.addAppendHook(appendHook);
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody((new byte[128]));
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assertions.assertEquals(i, resEntry.getIndex());
        }
        Assertions.assertEquals(9, fileStore.getLedgerEndIndex());
        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = fileStore.get(i);
            Assertions.assertEquals(i, entry.getIndex());
            Assertions.assertEquals(entry.getIndex(), ByteBuffer.wrap(entry.getBody()).getLong());
        }
    }

    @Test
    public void testAppendAsLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        DLedgerMmapFileStore fileStore = createFileStore(group, "n0-localhost:20911", "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(("Hello Leader" + i).getBytes());
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assertions.assertEquals(i, resEntry.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = fileStore.get(i);
            Assertions.assertEquals(i, entry.getIndex());
            Assertions.assertArrayEquals(("Hello Leader" + i).getBytes(), entry.getBody());
        }

        for (long i = 0; i < 10; i++) {
            fileStore.updateCommittedIndex(0, i);
            Assertions.assertEquals(i, fileStore.getCommittedIndex());
            DLedgerEntry entry = fileStore.get(i);
            Assertions.assertEquals(entry.getPos() + entry.getSize(), fileStore.getCommittedPos());
        }
        Assertions.assertEquals(fileStore.getCommittedPos(), fileStore.getDataFileList().getMaxWrotePosition());

        //ignore the smaller index and smaller term
        fileStore.updateCommittedIndex(0, -1);
        Assertions.assertEquals(9, fileStore.getLedgerEndIndex());
        fileStore.updateCommittedIndex(0, 0);
        Assertions.assertEquals(9, fileStore.getLedgerEndIndex());
        fileStore.updateCommittedIndex(-1, 10);
        Assertions.assertEquals(9, fileStore.getLedgerEndIndex());
    }

    @Test
    public void testNormalRecovery() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerMmapFileStore fileStore = createFileStore(group, peers, "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(("Hello Leader With Recovery" + i).getBytes());
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assertions.assertEquals(i, resEntry.getIndex());
        }
        while (fileStore.getFlushPos() != fileStore.getWritePos()) {
            fileStore.flush();
        }
        fileStore.shutdown();
        fileStore = createFileStore(group, peers, "n0", "n0");
        Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
        Assertions.assertEquals(9, fileStore.getLedgerEndIndex());
        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = fileStore.get(i);
            Assertions.assertEquals(i, entry.getIndex());
            Assertions.assertArrayEquals(("Hello Leader With Recovery" + i).getBytes(), entry.getBody());
        }
    }

    @Test
    public void testAbnormalRecovery() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        {
            DLedgerMmapFileStore fileStore = createFileStore(group, peers, "n0", "n0", 10 * 1024 + MIN_BLANK_LEN, 10 * DLedgerMmapFileStore.INDEX_UNIT_SIZE, 0);
            for (int i = 0; i < 100; i++) {
                DLedgerEntry entry = new DLedgerEntry();
                entry.setBody(new byte[1024]);
                DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
                Assertions.assertEquals(i, resEntry.getIndex());
            }
            Assertions.assertEquals(12, fileStore.getDataFileList().getMappedFiles().size());
            Assertions.assertEquals(99, fileStore.getLedgerEndIndex());
            Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
            while (fileStore.getFlushPos() != fileStore.getWritePos()) {
                fileStore.flush();
            }
            fileStore.shutdown();
        }
        {
            DLedgerMmapFileStore fileStore = createFileStore(group, peers, "n0", "n0", 10 * 1024 + MIN_BLANK_LEN, 10 * DLedgerMmapFileStore.INDEX_UNIT_SIZE, 2);
            Assertions.assertEquals(10, fileStore.getDataFileList().getMappedFiles().size());
            Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
            Assertions.assertEquals(89, fileStore.getLedgerEndIndex());
            for (long i = 0; i < 89; i++) {
                DLedgerEntry entry = fileStore.get(i);
                Assertions.assertEquals(i, entry.getIndex());
            }
            fileStore.shutdown();
        }
        {
            DLedgerMmapFileStore fileStore = createFileStore(group, peers, "n0", "n0", 10 * 1024 + MIN_BLANK_LEN, 10 * DLedgerMmapFileStore.INDEX_UNIT_SIZE, 10);
            Assertions.assertEquals(0, fileStore.getDataFileList().getMappedFiles().size());
            Assertions.assertEquals(-1, fileStore.getLedgerBeginIndex());
            Assertions.assertEquals(-1, fileStore.getLedgerEndIndex());
            fileStore.shutdown();
        }
    }

    @Test
    public void testTruncate() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerMmapFileStore fileStore = createFileStore(group, peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * DLedgerMmapFileStore.INDEX_UNIT_SIZE, 0);
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(new byte[1024]);
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assertions.assertEquals(i, resEntry.getIndex());
        }
        Assertions.assertEquals(2, fileStore.getDataFileList().getMappedFiles().size());
        Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
        Assertions.assertEquals(9, fileStore.getLedgerEndIndex());
        fileStore.getMemberState().changeToFollower(fileStore.getLedgerEndTerm(), "n0");

        DLedgerMmapFileStore otherFileStore = createFileStore(group, peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * DLedgerMmapFileStore.INDEX_UNIT_SIZE, 0);

        {
            //truncate the mid
            DLedgerEntry midEntry = otherFileStore.get(5L);
            Assertions.assertNotNull(midEntry);
            long midIndex = fileStore.truncate(midEntry, fileStore.getLedgerEndTerm(), "n0");
            Assertions.assertEquals(5, midIndex);
            Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
            Assertions.assertEquals(5, fileStore.getLedgerEndIndex());
            Assertions.assertEquals(midEntry.getPos() + midEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assertions.assertEquals((midIndex + 1) * DLedgerMmapFileStore.INDEX_UNIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }
        {
            //truncate just after
            DLedgerEntry afterEntry = otherFileStore.get(6L);
            Assertions.assertNotNull(afterEntry);
            long afterIndex = fileStore.truncate(afterEntry, fileStore.getLedgerEndTerm(), "n0");
            Assertions.assertEquals(6, afterIndex);
            Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
            Assertions.assertEquals(6, fileStore.getLedgerEndIndex());
            Assertions.assertEquals(afterEntry.getPos() + afterEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assertions.assertEquals((afterIndex + 1) * DLedgerMmapFileStore.INDEX_UNIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

        {
            //truncate to the end
            DLedgerEntry endEntry = otherFileStore.get(9L);
            Assertions.assertNotNull(endEntry);
            long endIndex = fileStore.truncate(endEntry, fileStore.getLedgerEndTerm(), "n0");
            Assertions.assertEquals(9, endIndex);
            Assertions.assertEquals(9, fileStore.getLedgerEndIndex());
            Assertions.assertEquals(9, fileStore.getLedgerBeginIndex());
            Assertions.assertEquals(endEntry.getPos() + endEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assertions.assertEquals((endIndex + 1) * DLedgerMmapFileStore.INDEX_UNIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }
    }

    @Test
    public void testAppendAsFollower() {
        DLedgerMmapFileStore fileStore = createFileStore(UUID.randomUUID().toString(), "n0-localhost:20913", "n0", "n1");
        long currPos = 0;
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setTerm(0);
            entry.setIndex(i);
            entry.setBody(("Hello Follower" + i).getBytes());
            entry.setPos(currPos);
            DLedgerEntry resEntry = fileStore.appendAsFollower(entry, 0, "n1");
            Assertions.assertEquals(i, resEntry.getIndex());
            currPos = currPos + entry.computeSizeInBytes();
        }
        for (long i = 0; i < 10; i++) {
            DLedgerEntry entry = fileStore.get(i);
            Assertions.assertEquals(i, entry.getIndex());
            Assertions.assertArrayEquals(("Hello Follower" + i).getBytes(), entry.getBody());
        }
    }

    @Test
    public void testReviseWherePosition() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerMmapFileStore fileStore = createFileStore(group, peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * DLedgerMmapFileStore.INDEX_UNIT_SIZE, 0);
        fileStore.shutdownFlushService();
        for (int i = 0; i < 20; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(new byte[1024]);
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            Assertions.assertEquals(i, resEntry.getIndex());
        }
        fileStore.getDataFileList().flush(0);

        Assertions.assertEquals(3, fileStore.getDataFileList().getMappedFiles().size());
        Assertions.assertEquals(0, fileStore.getLedgerBeginIndex());
        Assertions.assertEquals(19, fileStore.getLedgerEndIndex());
        fileStore.getMemberState().changeToFollower(fileStore.getLedgerEndTerm(), "n0");

        DLedgerMmapFileStore otherFileStore = createFileStore(group, peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * DLedgerMmapFileStore.INDEX_UNIT_SIZE, 0);

        {
            List<MmapFile> deleteFiles = new ArrayList<>();
            deleteFiles.add(fileStore.getDataFileList().getMappedFiles().get(0));
            deleteFiles.add(fileStore.getDataFileList().getMappedFiles().get(1));
            fileStore.getDataFileList().getMappedFiles().removeAll(deleteFiles);
            DLedgerEntry entry = otherFileStore.get(15L);
            Assertions.assertNotNull(entry);
            long index = fileStore.truncate(entry, fileStore.getLedgerEndTerm(), "n0");
            Assertions.assertEquals(15, index);
            Assertions.assertEquals(14, fileStore.getLedgerBeginIndex());
            Assertions.assertEquals(15, fileStore.getLedgerEndIndex());
            Assertions.assertEquals(entry.getPos() + entry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assertions.assertEquals((index + 1) * DLedgerMmapFileStore.INDEX_UNIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

        Assertions.assertTrue(fileStore.getFlushPos() >= fileStore.getDataFileList().getFirstMappedFile().getFileFromOffset());
    }

}
