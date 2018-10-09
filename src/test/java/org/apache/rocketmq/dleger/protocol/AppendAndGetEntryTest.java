package org.apache.rocketmq.dleger.protocol;

import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.client.DLegerClient;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class AppendAndGetEntryTest {

    private synchronized DLegerServer launchServer(String group, String peers, String selfId, String leaderId) {
        DLegerConfig config = new DLegerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.setStoreType(DLegerConfig.MEMORY);
        config.setEnableLeaderElector(false);
        DLegerServer dLegerServer = new DLegerServer(config);
        MemberState memberState = dLegerServer.getMemberState();
        memberState.setCurrTerm(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        dLegerServer.startup();
        return dLegerServer;
    }

    private synchronized DLegerClient launchClient(String group, String peers) {
        DLegerClient dLegerClient = new DLegerClient(peers);
        dLegerClient.startup();
        return dLegerClient;
    }

    @Test
    public void runSingleServer() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10001";
        launchServer(group, peers, selfId, selfId);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse  = dLegerClient.append(("HelloSingleServer" + i).getBytes());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloSingleServer" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void runThressServer() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10002;n1-localhost:10003;n2-localhost:10004";
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n1");
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n1");
        DLegerServer dLegerServer2 = launchServer(group, peers, "n2", "n1");
        DLegerClient dLegerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse  = dLegerClient.append(("HelloThreeServer" + i).getBytes());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(100);
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getCommittedIndex());
        Assert.assertEquals(9, dLegerServer1.getdLegerStore().getCommittedIndex());
        Assert.assertEquals(9, dLegerServer2.getdLegerStore().getCommittedIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloThreeServer" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }
}
