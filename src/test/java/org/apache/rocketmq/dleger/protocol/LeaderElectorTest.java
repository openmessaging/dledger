package org.apache.rocketmq.dleger.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.entry.ServerTestBase;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.Assert;
import org.junit.Test;



public class LeaderElectorTest extends ServerTestBase {

    private DLegerServer launchServer(String group, String peers, String selfId) {
        DLegerConfig config = new DLegerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLegerConfig.MEMORY);
        DLegerServer dLegerServer = new DLegerServer(config);
        dLegerServer.startup();
        bases.add(config.getDefaultPath());
        return dLegerServer;
    }

    @Test
    public void runSingleServer() throws Exception {
        String group = UUID.randomUUID().toString();
        DLegerServer dLegerServer = launchServer(group, "n0-localhost:10011", "n0");
        MemberState memberState =  dLegerServer.getMemberState();
        Thread.sleep(1000);
        Assert.assertTrue(memberState.isLeader());
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setBody("Hello Single Server".getBytes());
            AppendEntryResponse appendEntryResponse  = dLegerServer.getdLegerRpcService().append(appendEntryRequest).get();
            Assert.assertEquals(DLegerResponseCode.SUCCESS, appendEntryResponse.getCode());
        }
        long term = memberState.currTerm();
        dLegerServer.shutdown();
        dLegerServer = launchServer(group, "n0-localhost:10011", "n0");
        memberState = dLegerServer.getMemberState();
        Thread.sleep(1000);
        Assert.assertTrue(memberState.isLeader());
        Assert.assertEquals(term, memberState.currTerm());


    }

    @Test
    public void runThressServer() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10012;n1-localhost:10013;n2-localhost:10014";
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(1000);
        int leaderNum = 0, followerNum = 0;
        DLegerServer leaderServer = null;
        DLegerServer followerServer = null;
        for (DLegerServer server: servers) {
            if (server.getMemberState().isLeader()) {
                leaderNum++;
                leaderServer = server;
            } else if (server.getMemberState().isFollower()) {
                followerServer = server;
                followerNum++;
            }
        }
        Assert.assertEquals(1, leaderNum);
        Assert.assertEquals(2, followerNum);
        Assert.assertNotNull(leaderServer);
        for (int i = 0; i < 5; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setBody("Hello Single Server".getBytes());
            AppendEntryResponse appendEntryResponse  = leaderServer.getdLegerRpcService().append(appendEntryRequest).get();
            Assert.assertEquals(DLegerResponseCode.SUCCESS, appendEntryResponse.getCode());
        }

        //restart the follower, the leader should keep the same
        long term =  leaderServer.getMemberState().currTerm();
        String followerId = followerServer.getMemberState().getSelfId();
        followerServer.shutdown();
        followerServer = launchServer(group, peers, followerId);
        Thread.sleep(1000);
        Assert.assertTrue(followerServer.getMemberState().isFollower());
        Assert.assertTrue(leaderServer.getMemberState().isLeader());
        Assert.assertEquals(term, followerServer.getMemberState().currTerm());

    }
}

