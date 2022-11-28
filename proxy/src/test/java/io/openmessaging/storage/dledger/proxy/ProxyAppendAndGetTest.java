/*
 * Copyright 2017-2022 The DLedger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.proxy;

import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.common.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.common.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.common.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.common.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.common.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test the Multi-DLedger's append and get function
 */
public class ProxyAppendAndGetTest extends ServerTestHarness {

    @Test
    public void testThreeServerInOneDLedgerProxyInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        List<DLedgerConfig> configs = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            DLedgerConfig config = createDLedgerConfig(group, peers, "n" + i, "n1", DLedgerConfig.MEMORY);
            configs.add(config);
        }
        dLedgerProxyConfig.setConfigs(configs);
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        DLedgerServer dLedgerServer0 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n0");
        DLedgerServer dLedgerServer1 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n1");
        DLedgerServer dLedgerServer2 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n2");
        Assertions.assertNotNull(dLedgerServer0);
        Assertions.assertNotNull(dLedgerServer1);
        Assertions.assertNotNull(dLedgerServer2);
        Assertions.assertTrue(dLedgerServer1.getMemberState().isLeader());
        Thread.sleep(1000);
        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInOneDLedgerProxyInMemory" + i).getBytes());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1000);
        Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());
        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(("HelloThreeServerInOneDLedgerProxyInMemory" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThreeServerInOneDLedgerProxyInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        List<DLedgerConfig> configs = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            DLedgerConfig config = createDLedgerConfig(group, peers, "n" + i, "n1", DLedgerConfig.FILE);
            configs.add(config);
        }
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        dLedgerProxyConfig.setConfigs(configs);
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        DLedgerServer dLedgerServer0 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n0");
        DLedgerServer dLedgerServer1 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n1");
        DLedgerServer dLedgerServer2 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n2");
        Assertions.assertNotNull(dLedgerServer0);
        Assertions.assertNotNull(dLedgerServer1);
        Assertions.assertNotNull(dLedgerServer2);
        Assertions.assertTrue(dLedgerServer1.getMemberState().isLeader());
        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInOneDLedgerProxyInFile" + i).getBytes());
            Assertions.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(100);
        Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());
        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(("HelloThreeServerInOneDLedgerProxyInFile" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThreeServerInOneDLedgerProxyInFileWithAsyncRequests() throws Exception {
        String group = UUID.randomUUID().toString();
        int port = nextPort();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", port, port, port);
        List<DLedgerConfig> configs = new LinkedList<>();
        for (int i = 0; i < 3; i++) {
            DLedgerConfig config = createDLedgerConfig(group, peers, "n" + i, "n1", DLedgerConfig.FILE);
            configs.add(config);
        }
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        dLedgerProxyConfig.setConfigs(configs);
        DLedgerProxy dLedgerProxy = launchDLedgerProxy(dLedgerProxyConfig);
        DLedgerServer dLedgerServer0 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n0");
        DLedgerServer dLedgerServer1 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n1");
        DLedgerServer dLedgerServer2 = dLedgerProxy.getDLedgerManager().getDLedgerServer(group, "n2");
        Assertions.assertNotNull(dLedgerServer0);
        Assertions.assertNotNull(dLedgerServer1);
        Assertions.assertNotNull(dLedgerServer2);
        Assertions.assertTrue(dLedgerServer1.getMemberState().isLeader());
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(dLedgerServer1.getMemberState().getSelfId());
            request.setBody(("HelloThreeServerInOneDLedgerProxyInFileWithAsyncRequests" + i).getBytes());
            futures.add(dLedgerProxy.handleAppend(request));
        }
        Thread.sleep(1000);
        Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assertions.assertTrue(future.isDone());
            Assertions.assertEquals(i, future.get().getIndex());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), future.get().getCode());

            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            DLedgerEntry entry = getEntriesResponse.getEntries().get(0);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(("HelloThreeServerInOneDLedgerProxyInFileWithAsyncRequests" + i).getBytes(), entry.getBody());
            //assert the pos
            Assertions.assertEquals(entry.getPos(), future.get().getPos());
        }
    }

    /**
     * total three proxy, each proxy contains three servers and one of its servers will be a leader.
     *
     * @throws Exception
     */
    @Test
    public void testThreeProxyEachCarryThreeServerInMemory() throws Exception {
        String group0 = UUID.randomUUID().toString();
        String group1 = UUID.randomUUID().toString();
        String group2 = UUID.randomUUID().toString();
        String[] groups = new String[]{group0, group1, group2};
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        char[] prefix = new char[]{'a', 'b', 'c'};
        String peers0 = String.format("a0-localhost:%d;a1-localhost:%d;a2-localhost:%d", port1, port2, port3);
        String peers1 = String.format("b0-localhost:%d;b1-localhost:%d;b2-localhost:%d", port1, port2, port3);
        String peers2 = String.format("c0-localhost:%d;c1-localhost:%d;c2-localhost:%d", port1, port2, port3);
        String[] peers = new String[]{peers0, peers1, peers2};
        DLedgerProxyConfig[] dLedgerProxyConfigs = new DLedgerProxyConfig[3];
        List<DLedgerConfig>[] configArr = new LinkedList[3];
        for (int i = 0; i < 3; i++) {
            configArr[i] = new LinkedList<>();
        }
        for (int i = 0; i < 3; i++) {
            char selfPrefix = prefix[i];
            String leaderId = new String(new char[]{selfPrefix}) + i;
            for (int j = 0; j < 3; j++) {
                String selfId = new String(new char[]{selfPrefix}) + j;
                DLedgerConfig config = createDLedgerConfig(groups[i], peers[i], selfId, leaderId, DLedgerConfig.MEMORY);
                configArr[j].add(config);
            }
        }
        for (int i = 0; i < 3; i++) {
            dLedgerProxyConfigs[i] = new DLedgerProxyConfig();
            dLedgerProxyConfigs[i].setConfigs(configArr[i]);
        }
        System.out.println(dLedgerProxyConfigs);
        DLedgerProxy[] proxies = launchDLedgerProxy(dLedgerProxyConfigs);
        //get all leaders
        DLedgerServer dLedgerServerA0 = proxies[0].getDLedgerManager().getDLedgerServer(group0, "a0");
        DLedgerServer dLedgerServerB1 = proxies[1].getDLedgerManager().getDLedgerServer(group1, "b1");
        DLedgerServer dLedgerServerC2 = proxies[2].getDLedgerManager().getDLedgerServer(group2, "c2");
        DLedgerServer[] servers = new DLedgerServer[]{dLedgerServerA0, dLedgerServerB1, dLedgerServerC2};
        Assertions.assertTrue(dLedgerServerA0.getMemberState().isLeader());
        Assertions.assertTrue(dLedgerServerB1.getMemberState().isLeader());
        Assertions.assertTrue(dLedgerServerC2.getMemberState().isLeader());
        DLedgerClient dLedgerClient0 = launchClient(group0, peers0.split(";")[0]);
        DLedgerClient dLedgerClient1 = launchClient(group1, peers1.split(";")[0]);
        DLedgerClient dLedgerClient2 = launchClient(group2, peers2.split(";")[0]);
        DLedgerClient[] dLedgerClients = new DLedgerClient[]{dLedgerClient0, dLedgerClient1, dLedgerClient2};
        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 10; i++) {
                AppendEntryResponse appendEntryResponse = dLedgerClients[k].append(("HelloThreeProxyEachCarryThreeServerInMemory" + groups[k] + i).getBytes());
                Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
                Assertions.assertEquals(i, appendEntryResponse.getIndex());
            }
        }
        Thread.sleep(1000);
        for (int k = 0; k < 3; k++) {
            DLedgerServer dLedgerServer0 = proxies[k].getDLedgerManager().getDLedgerServer(groups[0], new String(new char[]{prefix[0]}) + k);
            DLedgerServer dLedgerServer1 = proxies[k].getDLedgerManager().getDLedgerServer(groups[1], new String(new char[]{prefix[1]}) + k);
            DLedgerServer dLedgerServer2 = proxies[k].getDLedgerManager().getDLedgerServer(groups[2], new String(new char[]{prefix[2]}) + k);
            Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
            Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
            Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());
        }

        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 10; i++) {
                GetEntriesResponse getEntriesResponse = dLedgerClients[k].get(i);
                Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
                Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
                Assertions.assertArrayEquals(("HelloThreeProxyEachCarryThreeServerInMemory" + groups[k] + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
            }
        }
    }

    @Test
    public void testThreeProxyEachCarryThreeServerInFile() throws Exception {
        String group0 = UUID.randomUUID().toString();
        String group1 = UUID.randomUUID().toString();
        String group2 = UUID.randomUUID().toString();
        String[] groups = new String[]{group0, group1, group2};
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        char[] prefix = new char[]{'a', 'b', 'c'};
        String peers0 = String.format("a0-localhost:%d;a1-localhost:%d;a2-localhost:%d", port1, port2, port3);
        String peers1 = String.format("b0-localhost:%d;b1-localhost:%d;b2-localhost:%d", port1, port2, port3);
        String peers2 = String.format("c0-localhost:%d;c1-localhost:%d;c2-localhost:%d", port1, port2, port3);
        String[] peers = new String[]{peers0, peers1, peers2};
        DLedgerProxyConfig[] dLedgerProxyConfigs = new DLedgerProxyConfig[3];
        List<DLedgerConfig>[] configArr = new LinkedList[3];
        for (int i = 0; i < 3; i++) {
            configArr[i] = new LinkedList<>();
        }
        for (int i = 0; i < 3; i++) {
            char selfPrefix = prefix[i];
            String leaderId = new String(new char[]{selfPrefix}) + i;
            for (int j = 0; j < 3; j++) {
                String selfId = new String(new char[]{selfPrefix}) + j;
                DLedgerConfig config = createDLedgerConfig(groups[i], peers[i], selfId, leaderId, DLedgerConfig.FILE);
                configArr[j].add(config);
            }
        }
        for (int i = 0; i < 3; i++) {
            dLedgerProxyConfigs[i] = new DLedgerProxyConfig();
            dLedgerProxyConfigs[i].setConfigs(configArr[i]);
        }
        System.out.println(dLedgerProxyConfigs);
        DLedgerProxy[] proxies = launchDLedgerProxy(dLedgerProxyConfigs);
        //get all leaders
        DLedgerServer dLedgerServerA0 = proxies[0].getDLedgerManager().getDLedgerServer(group0, "a0");
        DLedgerServer dLedgerServerB1 = proxies[1].getDLedgerManager().getDLedgerServer(group1, "b1");
        DLedgerServer dLedgerServerC2 = proxies[2].getDLedgerManager().getDLedgerServer(group2, "c2");
        DLedgerServer[] servers = new DLedgerServer[]{dLedgerServerA0, dLedgerServerB1, dLedgerServerC2};
        Assertions.assertTrue(dLedgerServerA0.getMemberState().isLeader());
        Assertions.assertTrue(dLedgerServerB1.getMemberState().isLeader());
        Assertions.assertTrue(dLedgerServerC2.getMemberState().isLeader());
        DLedgerClient dLedgerClient0 = launchClient(group0, peers0.split(";")[0]);
        DLedgerClient dLedgerClient1 = launchClient(group1, peers1.split(";")[0]);
        DLedgerClient dLedgerClient2 = launchClient(group2, peers2.split(";")[0]);
        DLedgerClient[] dLedgerClients = new DLedgerClient[]{dLedgerClient0, dLedgerClient1, dLedgerClient2};
        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 10; i++) {
                AppendEntryResponse appendEntryResponse = dLedgerClients[k].append(("HelloThreeProxyEachCarryThreeServerInFile" + groups[k] + i).getBytes());
                Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
                Assertions.assertEquals(i, appendEntryResponse.getIndex());
            }
        }
        Thread.sleep(1000);
        for (int k = 0; k < 3; k++) {
            DLedgerServer dLedgerServer0 = proxies[k].getDLedgerManager().getDLedgerServer(groups[0], new String(new char[]{prefix[0]}) + k);
            DLedgerServer dLedgerServer1 = proxies[k].getDLedgerManager().getDLedgerServer(groups[1], new String(new char[]{prefix[1]}) + k);
            DLedgerServer dLedgerServer2 = proxies[k].getDLedgerManager().getDLedgerServer(groups[2], new String(new char[]{prefix[2]}) + k);
            Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
            Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
            Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());
        }

        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 10; i++) {
                GetEntriesResponse getEntriesResponse = dLedgerClients[k].get(i);
                Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
                Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
                Assertions.assertArrayEquals(("HelloThreeProxyEachCarryThreeServerInFile" + groups[k] + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
            }
        }
    }

    @Test
    public void testTreeProxyEachCarryThreeServerInFileWithAsyncRequests() throws Exception {
        String group0 = UUID.randomUUID().toString();
        String group1 = UUID.randomUUID().toString();
        String group2 = UUID.randomUUID().toString();
        String[] groups = new String[]{group0, group1, group2};
        int port1 = nextPort();
        int port2 = nextPort();
        int port3 = nextPort();
        char[] prefix = new char[]{'a', 'b', 'c'};
        String peers0 = String.format("a0-localhost:%d;a1-localhost:%d;a2-localhost:%d", port1, port2, port3);
        String peers1 = String.format("b0-localhost:%d;b1-localhost:%d;b2-localhost:%d", port1, port2, port3);
        String peers2 = String.format("c0-localhost:%d;c1-localhost:%d;c2-localhost:%d", port1, port2, port3);
        String[] peers = new String[]{peers0, peers1, peers2};
        DLedgerProxyConfig[] dLedgerProxyConfigs = new DLedgerProxyConfig[3];
        List<DLedgerConfig>[] configArr = new LinkedList[3];
        for (int i = 0; i < 3; i++) {
            configArr[i] = new LinkedList<>();
        }
        for (int i = 0; i < 3; i++) {
            char selfPrefix = prefix[i];
            String leaderId = new String(new char[]{selfPrefix}) + i;
            for (int j = 0; j < 3; j++) {
                String selfId = new String(new char[]{selfPrefix}) + j;
                DLedgerConfig config = createDLedgerConfig(groups[i], peers[i], selfId, leaderId, DLedgerConfig.FILE);
                configArr[j].add(config);
            }
        }
        for (int i = 0; i < 3; i++) {
            dLedgerProxyConfigs[i] = new DLedgerProxyConfig();
            dLedgerProxyConfigs[i].setConfigs(configArr[i]);
        }
        System.out.println(dLedgerProxyConfigs);
        DLedgerProxy[] proxies = launchDLedgerProxy(dLedgerProxyConfigs);
        //get all leaders
        DLedgerServer dLedgerServerA0 = proxies[0].getDLedgerManager().getDLedgerServer(group0, "a0");
        DLedgerServer dLedgerServerB1 = proxies[1].getDLedgerManager().getDLedgerServer(group1, "b1");
        DLedgerServer dLedgerServerC2 = proxies[2].getDLedgerManager().getDLedgerServer(group2, "c2");
        DLedgerServer[] servers = new DLedgerServer[]{dLedgerServerA0, dLedgerServerB1, dLedgerServerC2};
        Assertions.assertTrue(dLedgerServerA0.getMemberState().isLeader());
        Assertions.assertTrue(dLedgerServerB1.getMemberState().isLeader());
        Assertions.assertTrue(dLedgerServerC2.getMemberState().isLeader());
        DLedgerClient dLedgerClient0 = launchClient(group0, peers0.split(";")[0]);
        DLedgerClient dLedgerClient1 = launchClient(group1, peers1.split(";")[0]);
        DLedgerClient dLedgerClient2 = launchClient(group2, peers2.split(";")[0]);
        DLedgerClient[] dLedgerClients = new DLedgerClient[]{dLedgerClient0, dLedgerClient1, dLedgerClient2};
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 10; i++) {
                AppendEntryRequest request = new AppendEntryRequest();
                request.setGroup(groups[k]);
                request.setRemoteId(servers[k].getMemberState().getSelfId());
                request.setBody(("HelloTreeProxyEachCarryThreeServerInFileWithAsyncRequests" + groups[k] + i).getBytes());
                futures.add(proxies[k].handleAppend(request));
            }
        }

        Thread.sleep(1000);
        for (int k = 0; k < 3; k++) {
            DLedgerServer dLedgerServer0 = proxies[k].getDLedgerManager().getDLedgerServer(groups[0], new String(new char[]{prefix[0]}) + k);
            DLedgerServer dLedgerServer1 = proxies[k].getDLedgerManager().getDLedgerServer(groups[1], new String(new char[]{prefix[1]}) + k);
            DLedgerServer dLedgerServer2 = proxies[k].getDLedgerManager().getDLedgerServer(groups[2], new String(new char[]{prefix[2]}) + k);
            Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
            Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
            Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());
        }

        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < futures.size() / 3; i++) {
                CompletableFuture<AppendEntryResponse> future = futures.get(i);
                Assertions.assertTrue(future.isDone());
                Assertions.assertEquals(i, future.get().getIndex());
                Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), future.get().getCode());
                GetEntriesResponse getEntriesResponse = dLedgerClients[k].get(i);
                DLedgerEntry entry = getEntriesResponse.getEntries().get(0);
                Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
                Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
                Assertions.assertArrayEquals(("HelloTreeProxyEachCarryThreeServerInFileWithAsyncRequests" + groups[k] + i).getBytes(), entry.getBody());
                //assert the pos
                Assertions.assertEquals(entry.getPos(), future.get().getPos());
            }
        }
    }

}
