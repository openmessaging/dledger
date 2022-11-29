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

package io.openmessaging.storage.core.snapshot;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.core.ServerTestHarness;
import io.openmessaging.storage.core.statemachine.MockStateMachine;
import io.openmessaging.storage.core.util.FileTestUtil;
import io.openmessaging.storage.dledger.utils.IOUtils;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import java.io.File;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SnapshotManagerTest extends ServerTestHarness {


    @Test
    public void testSaveAndLoadSnapshot() throws InterruptedException {
        //need fix
        // Launch server
      /*  String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerWithStateMachine(group, peers, "n0", "n1", DLedgerConfig.FILE, 10, 1024);
        DLedgerServer dLedgerServer1 = launchServerWithStateMachine(group, peers, "n1", "n1", DLedgerConfig.FILE, 10, 1024);
        DLedgerServer dLedgerServer2 = launchServerWithStateMachine(group, peers, "n2", "n1", DLedgerConfig.FILE, 10, 1024);
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>() {
            {
                add(dLedgerServer0);
                add(dLedgerServer1);
                add(dLedgerServer2);
            }
        };
        // Launch client
        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 100; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[512]);
            assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1200);
        for (DLedgerServer server : serverList) {
            assertEquals(99, server.getdLedgerStore().getLedgerEndIndex());
        }
        // Check state machine
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            assertEquals(99, fsm.getAppliedIndex());
            assertEquals(100, fsm.getTotalEntries());
        }
        Thread.sleep(100);
        // Shutdown server
        dLedgerServer0.shutdown();
        dLedgerServer1.shutdown();
        dLedgerServer2.shutdown();
        serverList.clear();
        // Restart server and apply snapshot
        DLedgerServer newDLedgerServer0 = launchServerWithStateMachine(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 1024);
        DLedgerServer newDLedgerServer1 = launchServerWithStateMachine(group, peers, "n1", "n0", DLedgerConfig.FILE, 10, 1024);
        DLedgerServer newDLedgerServer2 = launchServerWithStateMachine(group, peers, "n2", "n0", DLedgerConfig.FILE, 10, 1024);
        serverList.add(newDLedgerServer0);
        serverList.add(newDLedgerServer1);
        serverList.add(newDLedgerServer2);
        Thread.sleep(1000);
        // State machine could only be recovered from snapshot due to the entry has been removed after saving snapshot
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            Assertions.assertEquals(99, server.getFsmCaller().getLastAppliedIndex());
            assertEquals(100, fsm.getTotalEntries());
        }*/
    }

    @Test
    public void testSnapshotReservedNum() throws InterruptedException {
        //need fix
/*        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("%s-localhost:%d", selfId, nextPort());
        DLedgerServer server = launchServerWithStateMachine(group, peers, selfId, "n0", DLedgerConfig.FILE, 10, 1024);

        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < 120; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[512]);
            assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            assertEquals(i, appendEntryResponse.getIndex());
            Thread.sleep(100);
        }
        // Check snapshot reserved number
        int snapshotCnt = Objects.requireNonNull(new File(server.getDLedgerConfig().getSnapshotStoreBaseDir()).listFiles()).length;
        int maxSnapshotReservedNum = server.getDLedgerConfig().getMaxSnapshotReservedNum();
        assertEquals(snapshotCnt, maxSnapshotReservedNum);*/
    }

    @Test
    public void testLoadErrorSnapshot() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("%s-localhost:%d", selfId, nextPort());
        String snapshotBaseDirPrefix = FileTestUtil.TEST_BASE + File.separator + group + File.separator + "dledger-" +
                selfId + File.separator + "snapshot" + File.separator + SnapshotManager.SNAPSHOT_DIR_PREFIX;

        // Build error snapshot without state machine data
        long errorSnapshotIdx1 = 10;
        String errorSnapshotStoreBasePath1 = snapshotBaseDirPrefix + errorSnapshotIdx1;
        IOUtils.string2File(JSON.toJSONString(new SnapshotMeta(errorSnapshotIdx1, 1)),
                errorSnapshotStoreBasePath1 + File.separator + SnapshotManager.SNAPSHOT_META_FILE);

        // Build error snapshot without state machine meta
        long errorSnapshotIdx2 = 9;
        String errorSnapshotStoreBasePath2 = snapshotBaseDirPrefix + errorSnapshotIdx2;
        IOUtils.string2File("100", errorSnapshotStoreBasePath2 + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);

        long snapshotIdx = 8;
        String snapshotStoreBasePath = snapshotBaseDirPrefix + snapshotIdx;
        SnapshotMeta snapshotMeta = new SnapshotMeta(snapshotIdx, 1);
        IOUtils.string2File(JSON.toJSONString(snapshotMeta), snapshotStoreBasePath + File.separator + SnapshotManager.SNAPSHOT_META_FILE);
        IOUtils.string2File("80", snapshotStoreBasePath + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);

        DLedgerServer server = launchServerWithStateMachine(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 10 * 1024 * 1024);
        Thread.sleep(1000);

        StateMachineCaller caller = server.getFsmCaller();
        MockStateMachine fsm = (MockStateMachine) caller.getStateMachine();
        assertEquals(caller.getLastAppliedIndex(), 8);
        assertEquals(fsm.getTotalEntries(), 80);
        caller.shutdown();
    }
}
