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

import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.proxy.DLedgerProxy;
import io.openmessaging.storage.dledger.proxy.DLedgerProxyConfig;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerTestHarness extends ServerTestBase {

    protected synchronized DLedgerProxy launchDLedgerProxy(String group, String peers, String selfId) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.MEMORY);
        DLedgerProxy dLedgerProxy = new DLedgerProxy(config);
        dLedgerProxy.startup();
        bases.add(config.getDefaultPath());
        return dLedgerProxy;
    }

    protected synchronized DLedgerConfig createDLedgerConfig(String group, String peers, String selfId) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.MEMORY);
        bases.add(config.getDefaultPath());
        return config;
    }


    protected synchronized DLedgerProxy launchDLedgerProxy(String group, String peers, String selfId, String preferredLeaderId) {
        DLedgerConfig config = createDLedgerConfig(group, peers, selfId, preferredLeaderId);
        DLedgerProxy dLedgerProxy = new DLedgerProxy(config);
        dLedgerProxy.startup();
        return dLedgerProxy;
    }

    protected synchronized DLedgerConfig createDLedgerConfig(String group, String peers, String selfId, String preferredLeaderId) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.MEMORY);
        config.setPreferredLeaderIds(preferredLeaderId);
        bases.add(config.getDefaultPath());
        return config;
    }

    protected synchronized DLedgerProxy launchDLedgerProxy(String group, String peers, String selfId, String leaderId,
                                                           String storeType) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.setStoreType(storeType);
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);
        DLedgerProxy dLedgerProxy = new DLedgerProxy(config);
        MemberState memberState = dLedgerProxy.getDLedgerManager().getDLedgerServers().get(0).getMemberState();
        memberState.setCurrTermForTest(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        dLedgerProxy.startup();
        return dLedgerProxy;
    }

    protected synchronized DLedgerConfig createDLedgerConfig(String group, String peers, String selfId, String leaderId,
                                                             String storeType) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.setStoreType(storeType);
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);
        config.setPreferredLeaderIds(leaderId);
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        return config;
    }

    protected synchronized DLedgerProxy launchDLedgerProxy(DLedgerProxyConfig dLedgerProxyConfig) {
        DLedgerProxy dLedgerProxy = new DLedgerProxy(dLedgerProxyConfig.getConfigs());
        for (DLedgerServer dLedgerServer : dLedgerProxy.getDLedgerManager().getDLedgerServers()) {
            String leaderId = dLedgerServer.getdLedgerConfig().getPreferredLeaderIds();
            if (!dLedgerServer.getdLedgerConfig().isEnableLeaderElector() && leaderId != null) {
                dLedgerServer.getMemberState().setCurrTermForTest(0);
                if (dLedgerServer.getMemberState().getSelfId().equals(leaderId)) {
                    dLedgerServer.getMemberState().changeToLeader(0);
                } else {
                    dLedgerServer.getMemberState().changeToFollower(0, leaderId);
                }
            }
        }
        dLedgerProxy.startup();
        return dLedgerProxy;
    }

    protected synchronized DLedgerProxy[] launchDLedgerProxy(DLedgerProxyConfig[] dLedgerProxyConfigs) {
        DLedgerProxy[] proxies = new DLedgerProxy[dLedgerProxyConfigs.length];
        for (int i = 0; i < dLedgerProxyConfigs.length; i++) {
            DLedgerProxy dLedgerProxy = new DLedgerProxy(dLedgerProxyConfigs[i]);
            for (DLedgerServer dLedgerServer : dLedgerProxy.getDLedgerManager().getDLedgerServers()) {
                String leaderId = dLedgerServer.getdLedgerConfig().getPreferredLeaderIds();
                if (!dLedgerServer.getdLedgerConfig().isEnableLeaderElector() && leaderId != null) {
                    if (dLedgerServer.getMemberState().getSelfId().equals(leaderId)) {
                        dLedgerServer.getMemberState().changeToLeader(0);
                    } else {
                        dLedgerServer.getMemberState().changeToFollower(0, leaderId);
                    }
                }
            }
            dLedgerProxy.startup();
            proxies[i] = dLedgerProxy;
        }
        return proxies;
    }

    protected synchronized DLedgerServer launchServerEnableBatchPush(String group, String peers, String selfId, String leaderId,
                                                                     String storeType) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.setStoreType(storeType);
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);
        config.setEnableBatchPush(true);
        config.setMaxBatchPushSize(300);
        DLedgerServer dLedgerServer = new DLedgerServer(config);
        MemberState memberState = dLedgerServer.getMemberState();
        memberState.setCurrTermForTest(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        dLedgerServer.startup();
        return dLedgerServer;
    }

    protected synchronized DLedgerProxy launchDLedgerProxyEnableBatchPush(String group, String peers, String selfId, String leaderId,
                                                                          String storeType) {
        DLedgerProxyConfig dLedgerProxyConfig = new DLedgerProxyConfig();
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.setStoreType(storeType);
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);
        config.setEnableBatchPush(true);
        config.setMaxBatchPushSize(300);
        dLedgerProxyConfig.setConfigs(Arrays.asList(config));
        DLedgerProxy dLedgerProxy = new DLedgerProxy(dLedgerProxyConfig);
        MemberState memberState = dLedgerProxy.getDLedgerManager().getDLedgerServer(config.getGroup(), config.getSelfId()).getMemberState();
        memberState.setCurrTermForTest(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        dLedgerProxy.startup();
        return dLedgerProxy;
    }

    protected synchronized DLedgerClient launchClient(String group, String peers) {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        return dLedgerClient;
    }

    protected DLedgerServer parseServers(List<DLedgerServer> servers, AtomicInteger leaderNum,
                                         AtomicInteger followerNum) {
        DLedgerServer leaderServer = null;
        for (DLedgerServer server : servers) {
            if (server.getMemberState().isLeader()) {
                leaderNum.incrementAndGet();
                leaderServer = server;
                System.out.println("server:"+server.getdLedgerConfig().getSelfId());
            } else if (server.getMemberState().isFollower()) {
                followerNum.incrementAndGet();
            }
        }
        return leaderServer;
    }

    protected void simulatePartition(DLedgerServer server1, DLedgerServer server2) {
        server1.getMemberState().getPeerMap().put(server2.getMemberState().getSelfId(), null);
        server2.getMemberState().getPeerMap().put(server1.getMemberState().getSelfId(), null);
    }
}
