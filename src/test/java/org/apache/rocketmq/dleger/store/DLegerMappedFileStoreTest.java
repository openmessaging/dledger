package org.apache.rocketmq.dleger.store;

import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.entry.ServerTestBase;
import org.apache.rocketmq.dleger.store.file.DLegerMappedFileStore;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class DLegerMappedFileStoreTest extends ServerTestBase {




    private synchronized DLegerMappedFileStore createFileStore(String group, String peers, String selfId, String leaderId) {
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
        DLegerMappedFileStore fileStore  = new DLegerMappedFileStore(config, memberState);
        fileStore.startup();
        return fileStore;
    }

    @Test
    public void testAppendAsLeader() {
        DLegerMappedFileStore fileStore =  createFileStore(UUID.randomUUID().toString(),  "n0-localhost:20911", "n0", "n0");
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
    public void testAppendAsFollower() {
        DLegerMappedFileStore fileStore =  createFileStore(UUID.randomUUID().toString(),  "n0-localhost:20911", "n0", "n1");
        for (int i = 0; i < 10; i++) {
            DLegerEntry entry = new DLegerEntry();
            entry.setTerm(0);
            entry.setIndex(i);
            entry.setBody(("Hello Follower" + i).getBytes());
            long index = fileStore.appendAsFollower(entry, 0, "n1");
            Assert.assertEquals(i, index);
        }
        for (long i = 0; i < 10; i++) {
            DLegerEntry entry = fileStore.get(i);
            Assert.assertEquals(i, entry.getIndex());
            Assert.assertArrayEquals(("Hello Follower" +i).getBytes(), entry.getBody());
        }
    }

}
