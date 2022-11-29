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

package io.openmessaging.storage.core.statemachine;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.core.ServerTestHarness;
import io.openmessaging.storage.core.util.FileTestUtil;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.utils.IOUtils;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotStatus;
import io.openmessaging.storage.dledger.snapshot.file.FileSnapshotReader;
import io.openmessaging.storage.dledger.snapshot.hook.LoadSnapshotHook;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.Pair;
import java.io.File;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StateMachineCallerTest extends ServerTestHarness {

    private DLedgerConfig config;

    @Test
    public void testOnCommittedAndOnSnapshotSave() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String leaderId = "n0";
        String peers = String.format("%s-localhost:%d", selfId, nextPort());

        final DLedgerServer dLedgerServer = createDLedgerServer(group, peers, selfId, leaderId);
        final Pair<StateMachineCaller, MockStateMachine> result = mockCaller(dLedgerServer);
        updateFileStore((DLedgerMmapFileStore) dLedgerServer.getDLedgerStore(), 10);
        final StateMachineCaller caller = result.getKey();
        final MockStateMachine fsm = result.getValue();

        caller.onCommitted(9);
        Thread.sleep(1000);
        assertEquals(fsm.getAppliedIndex(), 9);
        assertEquals(fsm.getTotalEntries(), 10);

        // Check onSnapshotSave result
        String snapshotMetaJSON = IOUtils.file2String(this.config.getSnapshotStoreBaseDir() + File.separator +
                SnapshotManager.SNAPSHOT_DIR_PREFIX + fsm.getAppliedIndex() + File.separator +
                SnapshotManager.SNAPSHOT_META_FILE);
        SnapshotMeta snapshotMetaFromJSON = JSON.parseObject(snapshotMetaJSON, SnapshotMeta.class);
        assertEquals(snapshotMetaFromJSON.getLastIncludedIndex(), 9);
        assertEquals(snapshotMetaFromJSON.getLastIncludedTerm(), 0);
        String snapshotData = IOUtils.file2String(this.config.getSnapshotStoreBaseDir() + File.separator +
                SnapshotManager.SNAPSHOT_DIR_PREFIX + fsm.getAppliedIndex() + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        assertEquals(Long.parseLong(snapshotData), 10);
        caller.shutdown();
    }

    @Test
    public void testOnSnapshotLoad() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String leaderId = "n0";
        String peers = String.format("%s-localhost:%d", selfId, nextPort());

        final DLedgerServer dLedgerServer = createDLedgerServer(group, peers, selfId, leaderId);
        final Pair<StateMachineCaller, MockStateMachine> result = mockCaller(dLedgerServer);
        final StateMachineCaller caller = result.getKey();
        final MockStateMachine fsm = result.getValue();

        final long lastIncludedIndex = 10;
        String snapshotStoreBasePath = this.config.getSnapshotStoreBaseDir() + File.separator + SnapshotManager.SNAPSHOT_DIR_PREFIX + lastIncludedIndex;
        SnapshotMeta snapshotMeta = new SnapshotMeta(lastIncludedIndex, 1);
        IOUtils.string2File(JSON.toJSONString(snapshotMeta), snapshotStoreBasePath + File.separator + SnapshotManager.SNAPSHOT_META_FILE);
        IOUtils.string2File("90", snapshotStoreBasePath + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);

        SnapshotReader reader = new FileSnapshotReader(snapshotStoreBasePath);
        final CountDownLatch latch = new CountDownLatch(1);
        caller.onSnapshotLoad(new LoadSnapshotHook() {
            @Override
            public SnapshotReader getSnapshotReader() {
                return reader;
            }

            @Override
            public void registerSnapshotMeta(SnapshotMeta snapshotMeta) {

            }

            @Override
            public void doCallBack(SnapshotStatus status) {
                assertEquals(status.getCode(), SnapshotStatus.SUCCESS.getCode());
                latch.countDown();
            }
        });
        latch.await();
        assertEquals(caller.getLastAppliedIndex(), 10);
        assertEquals(fsm.getTotalEntries(), 90);
        caller.shutdown();
    }

    private DLedgerServer createDLedgerServer(String group, String peers, String selfId, String leaderId) {
        this.config = new DLedgerConfig();
        this.config.group(group).selfId(selfId).peers(peers);
        this.config.setStoreBaseDir(FileTestUtil.TEST_BASE + File.separator + group);
        this.config.setSnapshotThreshold(0);
        this.config.setStoreType(DLedgerConfig.FILE);
        this.config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        this.config.setEnableLeaderElector(false);
        this.config.setEnableDiskForceClean(false);
        this.config.setDiskSpaceRatioToForceClean(0.90f);
        DLedgerServer dLedgerServer = new DLedgerServer(this.config);
        MemberState memberState = dLedgerServer.getMemberState();
        memberState.setCurrTermForTest(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(this.config.getDataStorePath());
        bases.add(this.config.getIndexStorePath());
        bases.add(this.config.getDefaultPath());
        return dLedgerServer;
    }

    public Pair<StateMachineCaller, MockStateMachine> mockCaller(DLedgerServer server) {
        MockStateMachine fsm = new MockStateMachine();
        server.registerStateMachine(fsm);
        StateMachineCaller caller = server.getFsmCaller();
        caller.start();
        server.getDLedgerStore().startup();
        return new Pair<>(caller, (MockStateMachine) caller.getStateMachine());
    }

    private void updateFileStore(DLedgerMmapFileStore fileStore, int entryNum) {
        MemberState memberState = fileStore.getMemberState();
        memberState.changeToLeader(0);
        for (int i = 0; i < entryNum; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody((new byte[1024]));
            DLedgerEntry resEntry = fileStore.appendAsLeader(entry);
            assertEquals(i, resEntry.getIndex());
        }
        fileStore.updateCommittedIndex(memberState.currTerm(), entryNum - 1);
        while (fileStore.getFlushPos() != fileStore.getWritePos()) {
            fileStore.flush();
        }
    }

    @Test
    public void testServerWithStateMachine() throws InterruptedException {
        // need fix
/*        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerWithStateMachine(group, peers, "n0", "n1", DLedgerConfig.FILE, 0, 10 * 1024 * 1024);
        DLedgerServer dLedgerServer1 = launchServerWithStateMachine(group, peers, "n1", "n1", DLedgerConfig.FILE, 0, 10 * 1024 * 1024);
        DLedgerServer dLedgerServer2 = launchServerWithStateMachine(group, peers, "n2", "n1", DLedgerConfig.FILE, 0, 10 * 1024 * 1024);
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>() {
            {
                add(dLedgerServer0);
                add(dLedgerServer1);
                add(dLedgerServer2);
            }
        };

        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1200);
        for (DLedgerServer server : serverList) {
            assertEquals(9, server.getdLedgerStore().getLedgerEndIndex());
        }
        // Check state machine
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            assertEquals(9, fsm.getAppliedIndex());
            assertEquals(10, fsm.getTotalEntries());
        }*/
    }
}