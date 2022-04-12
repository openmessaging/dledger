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

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderElectorTest extends ServerTestHarness {

    @Test
    public void testSingleServer() throws Exception {
        String group = UUID.randomUUID().toString();
        DLedgerServer dLedgerServer = launchServer(group, String.format("n0-localhost:%d", nextPort()), "n0");
        MemberState memberState = dLedgerServer.getMemberState();
        Thread.sleep(1000);
        Assertions.assertTrue(memberState.isLeader());
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(dLedgerServer.getMemberState().getSelfId());
            appendEntryRequest.setBody("Hello Single Server".getBytes());
            AppendEntryResponse appendEntryResponse = dLedgerServer.getdLedgerRpcService().append(appendEntryRequest).get();
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
        long term = memberState.currTerm();
        dLedgerServer.shutdown();
        dLedgerServer = launchServer(group, "n0-localhost:10011", "n0");
        memberState = dLedgerServer.getMemberState();
        Thread.sleep(1000);
        Assertions.assertTrue(memberState.isLeader());
        Assertions.assertEquals(term, memberState.currTerm());

    }

    @Test
    public void testThreeServer() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLedgerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(2000);
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        DLedgerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());
        Assertions.assertNotNull(leaderServer);

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
            DLedgerServer candidate = servers.get(i % servers.size());
            candidate.getdLedgerLeaderElector().testRevote(maxTerm + 1);
            Thread.sleep(2000);
            leaderNum.set(0);
            followerNum.set(0);
            leaderServer = parseServers(servers, leaderNum, followerNum);
            Assertions.assertEquals(1, leaderNum.get());
            Assertions.assertEquals(2, followerNum.get());
            Assertions.assertNotNull(leaderServer);
            Assertions.assertTrue(candidate == leaderServer);
        }
        //write some data
        for (int i = 0; i < 5; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(leaderServer.getMemberState().getSelfId());
            appendEntryRequest.setBody("Hello Three Server".getBytes());
            AppendEntryResponse appendEntryResponse = leaderServer.getdLedgerRpcService().append(appendEntryRequest).get();
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
    }

    @Test
    public void testThreeServerAndRestartFollower() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLedgerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        Thread.sleep(1000);
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        DLedgerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());
        Assertions.assertNotNull(leaderServer);

        //restart the follower, the leader should keep the same
        long term = leaderServer.getMemberState().currTerm();
        for (DLedgerServer server : servers) {
            if (server == leaderServer) {
                continue;
            }
            String followerId = server.getMemberState().getSelfId();
            server.shutdown();
            server = launchServer(group, peers, followerId);
            Thread.sleep(2000);
            Assertions.assertTrue(server.getMemberState().isFollower());
            Assertions.assertTrue(leaderServer.getMemberState().isLeader());
            Assertions.assertEquals(term, server.getMemberState().currTerm());
        }
    }

    @Test
    public void testThreeServerAndShutdownLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLedgerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        while (parseServers(servers, leaderNum, followerNum) == null && DLedgerUtils.elapsed(start) < 1000) {
            Thread.sleep(100);
        }
        Thread.sleep(1000);
        leaderNum.set(0);
        followerNum.set(0);
        DLedgerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());
        Assertions.assertNotNull(leaderServer);

        //shutdown the leader, should elect another leader
        leaderServer.shutdown();
        Thread.sleep(1500);
        List<DLedgerServer> leftServers = new ArrayList<>();
        for (DLedgerServer server : servers) {
            if (server != leaderServer) {
                leftServers.add(server);
            }
        }
        start = System.currentTimeMillis();
        while (parseServers(leftServers, leaderNum, followerNum) == null && DLedgerUtils.elapsed(start) < 3 * leaderServer.getdLedgerConfig().getHeartBeatTimeIntervalMs()) {
            Thread.sleep(100);
        }
        Thread.sleep(1000);
        leaderNum.set(0);
        followerNum.set(0);
        Assertions.assertNotNull(parseServers(leftServers, leaderNum, followerNum));
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(1, followerNum.get());

    }

    @Test
    public void testThreeServerAndShutdownFollowers() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLedgerServer> servers = new ArrayList<>();
        servers.add(launchServer(group, peers, "n0"));
        servers.add(launchServer(group, peers, "n1"));
        servers.add(launchServer(group, peers, "n2"));
        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);

        long start = System.currentTimeMillis();
        while (parseServers(servers, leaderNum, followerNum) == null && DLedgerUtils.elapsed(start) < 1000) {
            Thread.sleep(100);
        }
        Thread.sleep(1000);
        leaderNum.set(0);
        followerNum.set(0);

        DLedgerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());
        Assertions.assertNotNull(leaderServer);

        //restart the follower, the leader should keep the same
        for (DLedgerServer server : servers) {
            if (server == leaderServer) {
                continue;
            }
            server.shutdown();
        }

        long term = leaderServer.getMemberState().currTerm();
        start = System.currentTimeMillis();
        while (leaderServer.getMemberState().isLeader() && DLedgerUtils.elapsed(start) < 4 * leaderServer.getdLedgerConfig().getHeartBeatTimeIntervalMs()) {
            Thread.sleep(100);
        }
        Assertions.assertTrue(leaderServer.getMemberState().isCandidate());
        Assertions.assertEquals(term, leaderServer.getMemberState().currTerm());
    }


    @Test
    public void testThreeServerAndPreferredLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        List<DLedgerServer> servers = new ArrayList<>();
        String preferredLeaderId = "n2";
        servers.add(launchServer(group, peers, "n0", preferredLeaderId));
        servers.add(launchServer(group, peers, "n1", preferredLeaderId));
        servers.add(launchServer(group, peers, "n2", preferredLeaderId));

        AtomicInteger leaderNum = new AtomicInteger(0);
        AtomicInteger followerNum = new AtomicInteger(0);

        long start = System.currentTimeMillis();
        while (parseServers(servers, leaderNum, followerNum) == null && DLedgerUtils.elapsed(start) < 1000) {
            Thread.sleep(100);
        }
        Thread.sleep(3000);
        leaderNum.set(0);
        followerNum.set(0);

        DLedgerServer leaderServer = parseServers(servers, leaderNum, followerNum);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(2, followerNum.get());
        Assertions.assertNotNull(leaderServer);

        Assertions.assertEquals(preferredLeaderId, leaderServer.getdLedgerConfig().getSelfId());

        //1. shutdown leader.
        leaderServer.shutdown();
        Thread.sleep(1500);
        List<DLedgerServer> leftServers = new ArrayList<>();
        for (DLedgerServer server : servers) {
            if (server != leaderServer) {
                leftServers.add(server);
            }
        }
        start = System.currentTimeMillis();
        while (parseServers(leftServers, leaderNum, followerNum) == null && DLedgerUtils.elapsed(start) < 3 * leaderServer.getdLedgerConfig().getHeartBeatTimeIntervalMs()) {
            Thread.sleep(100);
        }
        Thread.sleep(300);
        leaderNum.set(0);
        followerNum.set(0);
        leaderServer = parseServers(leftServers, leaderNum, followerNum);
        Assertions.assertNotNull(leaderServer);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertEquals(1, followerNum.get());


        //2. restart leader;
        long oldTerm = leaderServer.getMemberState().currTerm();
        DLedgerServer newPreferredNode = launchServer(group, peers, preferredLeaderId, preferredLeaderId);
        leftServers.add(newPreferredNode);

        leaderNum.set(0);
        followerNum.set(0);

        start = System.currentTimeMillis();
        while ( ((leaderServer = parseServers(leftServers, leaderNum, followerNum)) == null  || leaderServer.getMemberState().currTerm() == oldTerm)
            && DLedgerUtils.elapsed(start) < 3000) {
            Thread.sleep(100);
        }
        Thread.sleep(1500);
        leaderNum.set(0);
        followerNum.set(0);
        leaderServer = parseServers(leftServers, leaderNum, followerNum);
        Assertions.assertEquals(1, leaderNum.get());
        Assertions.assertTrue(followerNum.get() >= 1);
        Assertions.assertNotNull(leaderServer);
        Assertions.assertEquals(preferredLeaderId, leaderServer.getdLedgerConfig().getSelfId());
    }
}

