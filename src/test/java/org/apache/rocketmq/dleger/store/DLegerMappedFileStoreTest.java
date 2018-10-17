package org.apache.rocketmq.dleger.store;

import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.entry.ServerTestBase;
import org.apache.rocketmq.dleger.store.file.DLegerMmapFileStore;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class DLegerMappedFileStoreTest extends ServerTestBase {




    private synchronized DLegerMmapFileStore createFileStore(String group, String peers, String selfId, String leaderId) {
        DLegerConfig config = new DLegerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLegerConfig.MEMORY);
        config.setEnableLeaderElector(false);
        MemberState memberState = new MemberState(config);
        memberState.setCurrTerm(0);
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
            long index = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, index);
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
        String peers = String.format("n0-localhost:%d", ServerTestBase.PORT_COUNTER.incrementAndGet());
        DLegerMmapFileStore fileStore =  createFileStore(group,  peers, "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(("Hello Leader With Recovery" + i).getBytes());
            long index = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, index);
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
        String peers = String.format("n0-localhost:%d", ServerTestBase.PORT_COUNTER.incrementAndGet());
        DLegerMmapFileStore fileStore =  createFileStore(group,  peers, "n0", "n0");
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setBody(("Hello Leader With Truncate" + i).getBytes());
            long index = fileStore.appendAsLeader(entry);
            Assert.assertEquals(i, index);
        }
        Assert.assertEquals(0, fileStore.getLegerBeginIndex());
        Assert.assertEquals(9, fileStore.getLegerEndIndex());
        fileStore.getMemberState().changeToFollower(fileStore.getLegerEndTerm(), "n0");
        DLegerEntry truncateEntry = fileStore.get(5L);
        Assert.assertNotNull(truncateEntry);
        Assert.assertEquals(5, truncateEntry.getIndex());
        long truncateIndex = fileStore.truncate(truncateEntry, fileStore.getLegerEndTerm(), "n0");
        Assert.assertEquals(5, truncateIndex);
        Assert.assertEquals(0, fileStore.getLegerBeginIndex());
        Assert.assertEquals(5, fileStore.getLegerEndIndex());
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
            long index = fileStore.appendAsFollower(entry, 0, "n1");
            Assert.assertEquals(i, index);
            currPos = currPos + entry.computSizeInBytes();
        }
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Follower" +i).getBytes(), entry.getBody());
        }
    }

}
