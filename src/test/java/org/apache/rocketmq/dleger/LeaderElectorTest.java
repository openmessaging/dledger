package org.apache.rocketmq.dleger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.junit.Assert;
import org.junit.Test;



public class LeaderElectorTest extends ServerTestHarness {

    @Test
    public void testSingleServer() throws Exception {
        String group = UUID.randomUUID().toString();
        DLegerServer dLegerServer = launchServer(group, "n0-localhost:10011", "n0");
        MemberState memberState =  dLegerServer.getMemberState();
        Thread.sleep(1000);
        Assert.assertTrue(memberState.isLeader());
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setRemoteId(dLegerServer.getMemberState().getSelfId());
            appendEntryRequest.setBody("Hello Single Server".getBytes());
            AppendEntryResponse appendEntryResponse  = dLegerServer.getdLegerRpcService().append(appendEntryRequest).get();
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
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
    public void testThressServer() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10012;n1-localhost:10013;n2-localhost:10014";
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(1000);
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        DLegerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(2, followerNum.get());
        Assert.assertNotNull(leaderServer);

        for (int i = 0; i < 10; i++) {
            long maxTerm = servers.stream().max((o1, o2) -> {
                if (o1.getMemberState().currTerm() < o2.getMemberState().currTerm()) {
                    return -1;
                } else if (o1.getMemberState().currTerm() > o2.getMemberState().currTerm()) {
                    return 1;
                } else {
                    return 0;
                }
            }).get().getMemberState().currTerm();
            DLegerServer candidate = servers.get( i % servers.size());
            candidate.getdLegerLeaderElector().testRevote(maxTerm + 1);
            Thread.sleep(100);
            leaderNum.set(0);
            followerNum.set(0);
            leaderServer = parseServers(servers, leaderNum, followerNum);
            Assert.assertEquals(1, leaderNum.get());
            Assert.assertEquals(2, followerNum.get());
            Assert.assertNotNull(leaderServer);
            Assert.assertTrue(candidate == leaderServer);
        }
        //write some data
        for (int i = 0; i < 5; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setRemoteId(leaderServer.getMemberState().getSelfId());
            appendEntryRequest.setBody("Hello Three Server".getBytes());
            AppendEntryResponse appendEntryResponse  = leaderServer.getdLegerRpcService().append(appendEntryRequest).get();
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
    }


    @Test
    public void testThressServerAndRestartFollower() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10015;n1-localhost:10016;n2-localhost:10017";
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(1000);
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        DLegerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(2, followerNum.get());
        Assert.assertNotNull(leaderServer);


        //restart the follower, the leader should keep the same
        long term =  leaderServer.getMemberState().currTerm();
        for (DLegerServer server : servers) {
            if (server == leaderServer) {
                continue;
            }
            String followerId = server.getMemberState().getSelfId();
            server.shutdown();
            server = launchServer(group, peers, followerId);
            Thread.sleep(1000);
            Assert.assertTrue(server.getMemberState().isFollower());
            Assert.assertTrue(leaderServer.getMemberState().isLeader());
            Assert.assertEquals(term, server.getMemberState().currTerm());
        }
    }


    @Test
    public void testThressServerAndRestartLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10018;n1-localhost:10019;n2-localhost:10020";
        List<DLegerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(1000);
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        DLegerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(2, followerNum.get());
        Assert.assertNotNull(leaderServer);

        //restart the leader, should elect another leader
        leaderServer.shutdown();
        Thread.sleep(1500);
        List<DLegerServer> leftServers = new ArrayList<>();
        for (DLegerServer server: servers) {
            if (server != leaderServer) {
                leftServers.add(server);
            }
        }
        leaderNum.set(0);
        followerNum.set(0);
        Assert.assertNotNull(parseServers(leftServers, leaderNum, followerNum));
        Assert.assertEquals(1, leaderNum.get());
        Assert.assertEquals(1, followerNum.get());

    }
}

