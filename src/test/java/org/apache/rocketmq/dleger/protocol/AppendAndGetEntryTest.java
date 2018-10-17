package org.apache.rocketmq.dleger.protocol;

import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.client.DLegerClient;
import org.apache.rocketmq.dleger.entry.ServerTestBase;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class AppendAndGetEntryTest extends ServerTestBase {

    private synchronized DLegerServer launchServer(String group, String peers, String selfId, String leaderId, String storeType) {
        DLegerConfig config = new DLegerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.setStoreType(storeType);
        config.setEnableLeaderElector(false);
        DLegerServer dLegerServer = new DLegerServer(config);
        MemberState memberState = dLegerServer.getMemberState();
        memberState.setCurrTerm(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        dLegerServer.startup();
        return dLegerServer;
    }

    private synchronized DLegerClient launchClient(String group, String peers) {
        DLegerClient dLegerClient = new DLegerClient(peers);
        dLegerClient.startup();
        return dLegerClient;
    }

    @Test
    public void runSingleServerInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10001";
        launchServer(group, peers, selfId, selfId, DLegerConfig.MEMORY);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse  = dLegerClient.append(("HelloSingleServerInMemory" + i).getBytes());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloSingleServerInMemory" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void runSingleServerInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10002";
        launchServer(group, peers, selfId, selfId, DLegerConfig.FILE);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse  = dLegerClient.append(("HelloSingleServerInFile" + i).getBytes());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloSingleServerInFile" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }



    @Test
    public void runThressServerInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10003;n1-localhost:10004;n2-localhost:10005";
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n1", DLegerConfig.MEMORY);
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n1", DLegerConfig.MEMORY);
        DLegerServer dLegerServer2 = launchServer(group, peers, "n2", "n1", DLegerConfig.MEMORY);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse  = dLegerClient.append(("HelloThreeServerInMemory" + i).getBytes());
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
            Assert.assertArrayEquals(("HelloThreeServerInMemory" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void runThressServerInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10006;n1-localhost:10007;n2-localhost:10008";
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer2 = launchServer(group, peers, "n2", "n1", DLegerConfig.FILE);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse  = dLegerClient.append(("HelloThreeServerInFile" + i).getBytes());
            Assert.assertEquals(appendEntryResponse.getCode(), DLegerResponseCode.SUCCESS);
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
            Assert.assertArrayEquals(("HelloThreeServerInFile" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }
}
