/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerTestHarness extends ServerTestBase {

    protected synchronized DLedgerServer launchServer(String group, String peers, String selfId) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer = new DLedgerServer(config);
        dLedgerServer.startup();
        bases.add(config.getDefaultPath());
        return dLedgerServer;
    }

    protected synchronized DLedgerServer launchServer(String group, String peers, String selfId, String preferredLeaderId) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.MEMORY);
        config.setPreferredLeaderId(preferredLeaderId);
        DLedgerServer dLedgerServer = new DLedgerServer(config);
        dLedgerServer.startup();
        bases.add(config.getDefaultPath());
        return dLedgerServer;
    }

    protected synchronized DLedgerServer launchServer(String group, String peers, String selfId, String leaderId,
        String storeType) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        config.setStoreType(storeType);
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);
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
