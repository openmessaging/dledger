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
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerTestHarness extends ServerTestBase {
    protected String getBaseDir() {
        return FileTestUtil.TEST_BASE;
    }
    protected synchronized DLedgerServer launchServer(String group, String peers, String selfId) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(getBaseDir() + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.FILE);
        DLedgerServer dLedgerServer = new DLedgerServer(config);
        dLedgerServer.startup();
        bases.add(config.getDefaultPath());
        return dLedgerServer;
    }

    protected synchronized DLedgerServer launchServer(String group, String peers, String selfId,
        String preferredLeaderId) {
        return launchServer(group, peers, selfId, preferredLeaderId, false);
    }

    protected synchronized DLedgerServer launchServer(String group, String peers, String selfId,
        String preferredLeaderId, boolean enbaleFastAdvanceCommitIndex) {
        DLedgerConfig config = new DLedgerConfig();
        config.setStoreBaseDir(getBaseDir() + File.separator + group);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLedgerConfig.FILE);
        config.setPreferredLeaderId(preferredLeaderId);
        config.setEnableFastAdvanceCommitIndex(enbaleFastAdvanceCommitIndex);
        DLedgerServer dLedgerServer = new DLedgerServer(config);
        dLedgerServer.startup();
        bases.add(config.getDefaultPath());
        return dLedgerServer;
    }

    protected synchronized DLedgerServer launchServer(String group, String peers, String selfId, String leaderId,
        String storeType) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(getBaseDir() + File.separator + group);
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


    protected DLedgerServer launchServerWithStateMachineDisableSnapshot(String group, String peers,
        String selfIf, String leaderId, String storeType, int mappedFileSizeForEntryData, StateMachine stateMachine) {
        return this.launchServerWithStateMachine(group, peers, selfIf, leaderId, storeType, false, 0,
            mappedFileSizeForEntryData, stateMachine);
    }

    protected DLedgerServer launchServerWithStateMachineEnableSnapshot(String group, String peers,
        String selfId, String leaderId, String storeType, int snapshotThreshold, int mappedFileSizeForEntryData,
        StateMachine stateMachine) {
        return this.launchServerWithStateMachine(group, peers, selfId, leaderId, storeType, true, snapshotThreshold,
            mappedFileSizeForEntryData, stateMachine);
    }

    protected synchronized DLedgerServer launchServerWithStateMachine(String group, String peers,
        String selfId, String leaderId, String storeType, boolean enableSnapshot, int snapshotThreshold, int mappedFileSizeForEntryData,
        StateMachine stateMachine) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(getBaseDir() + File.separator + group);
        config.setStoreType(storeType);
        config.setEnableSnapshot(enableSnapshot);
        config.setSnapshotThreshold(snapshotThreshold);
        config.setMappedFileSizeForEntryData(mappedFileSizeForEntryData);
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
        dLedgerServer.registerStateMachine(stateMachine);
        dLedgerServer.startup();
        return dLedgerServer;
    }

    protected synchronized DLedgerServer launchServerEnableBatchPush(String group, String peers, String selfId,
        String leaderId, String storeType) {
        DLedgerConfig config = new DLedgerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(getBaseDir() + File.separator + group);
        config.setStoreType(storeType);
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);
        config.setEnableBatchAppend(true);
        config.setMaxBatchAppendSize(300);
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
