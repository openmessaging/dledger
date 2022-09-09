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

package io.openmessaging.storage.dledger.proxy;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProxyLeaderElectorTest extends ServerTestHarness {

    @Test
    public void testThreeServerInOneProxy() throws Exception {
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        List<DLedgerServer> servers = new ArrayList<>();
        DLedgerConfig config0 = createDLedgerConfig(group, peers, "n0");
        DLedgerConfig config1 = createDLedgerConfig(group, peers, "n1");
        DLedgerConfig config2 = createDLedgerConfig(group, peers, "n2");
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        dLedgerProxyConfig.setConfigs(Arrays.asList(config0, config1, config2));
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        servers.addAll(dLedgerProxy.getDLedgerManager().getDLedgerServers());
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
            appendEntryRequest.setBody("Hello Three Server In One Proxy".getBytes());
            AppendEntryResponse appendEntryResponse = leaderServer.getdLedgerRpcService().append(appendEntryRequest).get();
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
    }

    @Test
    public void testThreeServerInOneProxyAndRestartFollower() throws Exception {
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        List<DLedgerServer> servers = new ArrayList<>();
        DLedgerConfig config0 = createDLedgerConfig(group, peers, "n0");
        DLedgerConfig config1 = createDLedgerConfig(group, peers, "n1");
        DLedgerConfig config2 = createDLedgerConfig(group, peers, "n2");
        List<DLedgerConfig> configs = Arrays.asList(config0, config1, config2);
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        dLedgerProxyConfig.setConfigs(Arrays.asList(config0, config1, config2));
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        servers.addAll(dLedgerProxy.getDLedgerManager().getDLedgerServers());
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
        long term = leaderServer.getMemberState().currTerm();
        for (DLedgerServer server : servers) {
            if (server == leaderServer) {
                continue;
            }
            String followerId = server.getMemberState().getSelfId();
            dLedgerProxy.removeDLedgerServer(server.getMemberState().getGroup(), followerId);
            dLedgerProxy.addDLedgerServer(server.getdLedgerConfig());
            Thread.sleep(2000);
            Assertions.assertTrue(server.getMemberState().isFollower());
            Assertions.assertTrue(leaderServer.getMemberState().isLeader());
            Assertions.assertEquals(term, server.getMemberState().currTerm());
        }
    }


    @Test
    public void testThreeServerInOneProxyAndShutdownLeader() throws Exception {
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        List<DLedgerServer> servers = new ArrayList<>();
        DLedgerConfig config0 = createDLedgerConfig(group, peers, "n0");
        DLedgerConfig config1 = createDLedgerConfig(group, peers, "n1");
        DLedgerConfig config2 = createDLedgerConfig(group, peers, "n2");
        List<DLedgerConfig> configs = Arrays.asList(config0, config1, config2);
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        dLedgerProxyConfig.setConfigs(configs);
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        servers.addAll(dLedgerProxy.getDLedgerManager().getDLedgerServers());
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

        //remove and shutdown the leader, should elect another leader
        dLedgerProxy.removeDLedgerServer(leaderServer.getMemberState().getGroup(), leaderServer.getMemberState().getSelfId());
        Thread.sleep(1500);
        List<DLedgerServer> leftServers = new ArrayList<>();
        for (DLedgerServer server : servers) {
            if (server != leaderServer) {
                leftServers.add(server);
            }
        }
        start = System.currentTimeMillis();
        while (parseServers(leftServers, leaderNum, followerNum) == null && DLedgerUtils.elapsed(start) < 3 * leaderServer.getDLedgerConfig().getHeartBeatTimeIntervalMs()) {
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
    public void testThreeServerInOneProxyAndShutdownFollowers() throws Exception {
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        List<DLedgerServer> servers = new ArrayList<>();
        DLedgerConfig config0 = createDLedgerConfig(group, peers, "n0");
        DLedgerConfig config1 = createDLedgerConfig(group, peers, "n1");
        DLedgerConfig config2 = createDLedgerConfig(group, peers, "n2");
        List<DLedgerConfig> configs = Arrays.asList(config0, config1, config2);
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        dLedgerProxyConfig.setConfigs(configs);
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        servers.addAll(dLedgerProxy.getDLedgerManager().getDLedgerServers());
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

        //shutdown the follower, the leader should keep the same
        for (DLedgerServer server : servers) {
            if (server == leaderServer) {
                continue;
            }
            dLedgerProxy.removeDLedgerServer(server.getMemberState().getGroup(), server.getMemberState().getSelfId());
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
    public void testThreeServerInOneProxyAndPreferredLeader() throws Exception {
        String preferredLeaderId = "n2";
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        List<DLedgerServer> servers = new ArrayList<>();
        DLedgerConfig config0 = createDLedgerConfig(group, peers, "n0", preferredLeaderId);
        DLedgerConfig config1 = createDLedgerConfig(group, peers, "n1", preferredLeaderId);
        DLedgerConfig config2 = createDLedgerConfig(group, peers, "n2", preferredLeaderId);
        List<DLedgerConfig> configs = Arrays.asList(config0, config1, config2);
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        dLedgerProxyConfig.setConfigs(configs);
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        servers.addAll(dLedgerProxy.getDLedgerManager().getDLedgerServers());
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

        //1.remove and shutdown leader.
        dLedgerProxy.removeDLedgerServer(leaderServer.getMemberState().getGroup(), leaderServer.getMemberState().getSelfId());
        DLedgerConfig preferredLeaderConfig = leaderServer.getdLedgerConfig();
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
        dLedgerProxy.addDLedgerServer(preferredLeaderConfig);
        Thread.sleep(500);
        DLedgerServer newPreferredNode = dLedgerProxy.getDLedgerManager().getDLedgerServer(preferredLeaderConfig.getGroup(), preferredLeaderConfig.getSelfId());
        leftServers.add(newPreferredNode);
        leaderNum.set(0);
        followerNum.set(0);
        start = System.currentTimeMillis();
        while (((leaderServer = parseServers(leftServers, leaderNum, followerNum)) == null || leaderServer.getMemberState().currTerm() == oldTerm)
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
        Assertions.assertEquals(preferredLeaderId, leaderServer.getDLedgerConfig().getSelfId());
    }
}

