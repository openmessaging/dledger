package org.apache.rocketmq.dleger.store;

import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.ServerTestHarness;
import org.apache.rocketmq.dleger.store.file.DLegerMmapFileStore;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.rocketmq.dleger.store.file.DLegerMmapFileStore.INDEX_NUIT_SIZE;
import static org.apache.rocketmq.dleger.store.file.MmapFileList.MIN_BLANK_LEN;

public class DLegerMappedFileStoreTest extends ServerTestHarness {




    private synchronized DLegerMmapFileStore createFileStore(String group, String peers, String selfId, String leaderId) {
       return createFileStore(group, peers, selfId, leaderId, 1024 * 1024 * 1024, INDEX_NUIT_SIZE * 1024 * 1024);
    }

    private synchronized DLegerMmapFileStore createFileStore(String group, String peers, String selfId, String leaderId,
        int dataFileSize, int indexFileSize) {
        DLegerConfig config = new DLegerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLegerConfig.MEMORY);
        config.setEnableLeaderElector(false);
        if (dataFileSize != -1) {
            config.setMappedFileSizeForEntryData(dataFileSize);
        }
        if (indexFileSize != -1) {
            config.setMappedFileSizeForEntryIndex(indexFileSize);
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
        DLegerMmapFileStore fileStore  = new DLegerMmapFileStore(config, memberState);
        fileStore.startup();
        return fileStore;
    }

    @Test
    public void testAppendAsLeader() {
        DLegerMmapFileStore fileStore =  createFileStore(UUID.randomUUID().toString(),  "n0-localhost:20911", "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(("Hello Leader" + i).getBytes());
            DLegerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Leader" +i).getBytes(), entry.getBody());
        }
    }


    @Test
    public void testRecovery() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLegerMmapFileStore fileStore =  createFileStore(group,  peers, "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(("Hello Leader With Recovery" + i).getBytes());
            DLegerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        while (fileStore.getFlushPos() != fileStore.getWritePos()) {
            fileStore.flush();
        }
        fileStore.shutdown();
        fileStore = createFileStore(group,  peers, "n0", "n0");
        Assert.assertEquals(0, fileStore.getLegerBeginIndex());
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Leader With Recovery" +i).getBytes(), entry.getBody());
        }
    }


    @Test
    public void testTruncate() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        DLegerMmapFileStore fileStore =  createFileStore(group,  peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * INDEX_NUIT_SIZE);
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(new byte[1024]);
            DLegerEntry resEntry = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        Assert.assertEquals(2, fileStore.getDataFileList().getMappedFiles().size());
        Assert.assertEquals(0, fileStore.getLegerBeginIndex());
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        fileStore.getMemberState().changeToFollower(fileStore.getLegerEndTerm(), "n0");


        DLegerMmapFileStore otherFileStore =  createFileStore(group,  peers, "n0", "n0", 8 * 1024 + MIN_BLANK_LEN, 8 * INDEX_NUIT_SIZE);

        {
            //truncate the mid
            DLegerEntry midEntry = otherFileStore.get(5L);
            Assert.assertNotNull(midEntry);
            long midIndex = fileStore.truncate(midEntry, fileStore.getLegerEndTerm(), "n0");
            Assert.assertEquals(5, midIndex);
            Assert.assertEquals(0, fileStore.getLegerBeginIndex());
            Assert.assertEquals(5, fileStore.getLegerEndIndex());
            Assert.assertEquals(midEntry.getPos() + midEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assert.assertEquals((midIndex + 1) * INDEX_NUIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }
        {
            //truncate just after
            DLegerEntry afterEntry = otherFileStore.get(6L);
            Assert.assertNotNull(afterEntry);
            long afterIndex = fileStore.truncate(afterEntry, fileStore.getLegerEndTerm(), "n0");
            Assert.assertEquals(6, afterIndex);
            Assert.assertEquals(0, fileStore.getLegerBeginIndex());
            Assert.assertEquals(6, fileStore.getLegerEndIndex());
            Assert.assertEquals(afterEntry.getPos() + afterEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assert.assertEquals((afterIndex + 1) * INDEX_NUIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

        {
            //truncate to the end
            DLegerEntry endEntry = otherFileStore.get(9L);
            Assert.assertNotNull(endEntry);
            long endIndex = fileStore.truncate(endEntry, fileStore.getLegerEndTerm(), "n0");
            Assert.assertEquals(9, endIndex);
            Assert.assertEquals(9, fileStore.getLegerEndIndex());
            Assert.assertEquals(9, fileStore.getLegerBeginIndex());
            Assert.assertEquals(endEntry.getPos() + endEntry.getSize(), fileStore.getDataFileList().getMaxWrotePosition());
            Assert.assertEquals((endIndex + 1) * INDEX_NUIT_SIZE, fileStore.getIndexFileList().getMaxWrotePosition());
        }

    }


    @Test
    public void testAppendAsFollower() {
        DLegerMmapFileStore fileStore =  createFileStore(UUID.randomUUID().toString(),  "n0-localhost:20913", "n0", "n1");
        long currPos = 0;
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setTerm(0);
            entry.setIndex(i);
            entry.setBody(("Hello Follower" + i).getBytes());
            entry.setPos(currPos);
            DLegerEntry resEntry = fileStore.appendAsFollower(entry, 0, "n1");
            Assert.assertEquals(i, resEntry.getIndex());
            currPos = currPos + entry.computSizeInBytes();
        }
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Follower" +i).getBytes(), entry.getBody());
        }
    }

}
