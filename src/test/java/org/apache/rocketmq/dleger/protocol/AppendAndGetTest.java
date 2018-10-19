package org.apache.rocketmq.dleger.protocol;

import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.client.DLegerClient;
import org.apache.rocketmq.dleger.ServerTestBase;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class AppendAndGetTest extends ServerTestHarness {



    @Test
    public void testSingleServerInMemory() throws Exception {
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
    public void testSingleServerInFile() throws Exception {
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
    public void testThressServerInMemory() throws Exception {
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
    public void testThressServerInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10006;n1-localhost:10007;n2-localhost:10008";
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer2 = launchServer(group, peers, "n2", "n1", DLegerConfig.FILE);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse  = dLegerClient.append(("HelloThreeServerInFile" + i).getBytes());
            Assert.assertEquals(appendEntryResponse.getCode(), DLegerResponseCode.SUCCESS.getCode());
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
