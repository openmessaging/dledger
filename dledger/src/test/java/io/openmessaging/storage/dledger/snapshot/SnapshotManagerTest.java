package io.openmessaging.storage.dledger.snapshot;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.ServerTestHarness;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.statemachine.MockStateMachine;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import io.openmessaging.storage.dledger.utils.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SnapshotManagerTest extends ServerTestHarness {

    public static final String STORE_PATH = FileTestUtil.createTestDir("SnapshotManagerTest");

    @Override
    protected String getBaseDir() {
        return STORE_PATH;
    }

    @Test
    public void testSaveAndLoadSnapshot() throws InterruptedException {
        // Launch server
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerWithStateMachineEnableSnapshot(group, peers, "n0", "n1", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        DLedgerServer dLedgerServer1 = launchServerWithStateMachineEnableSnapshot(group, peers, "n1", "n1", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        DLedgerServer dLedgerServer2 = launchServerWithStateMachineEnableSnapshot(group, peers, "n2", "n1", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>(3);
        serverList.add(dLedgerServer0);
        serverList.add(dLedgerServer1);
        serverList.add(dLedgerServer2);
        // Launch client
        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        // append 99 entries, each 10 entries will trigger one snapshotting
        for (int i = 0; i < 99; i++) {
            if (i % 10 == 0) {
                Thread.sleep(200);
            }
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[512]);
            assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(2000);
        for (DLedgerServer server : serverList) {
            MockStateMachine stateMachine = (MockStateMachine) server.getStateMachine();
            assertEquals(98, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(stateMachine.getLastSnapshotIncludedIndex(), server.getDLedgerStore().getLedgerBeforeBeginIndex());
            // check statemachine
            assertEquals(99, stateMachine.getTotalEntries());
        }

        // now restart, expect to load the latest snapshot and replay the entries after loaded snapshot
        // Shutdown server
        dLedgerServer0.shutdown();
        dLedgerServer1.shutdown();
        dLedgerServer2.shutdown();
        serverList.clear();
        // Restart server and apply snapshot
        dLedgerServer0 = launchServerWithStateMachineEnableSnapshot(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        dLedgerServer1 = launchServerWithStateMachineEnableSnapshot(group, peers, "n1", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        dLedgerServer2 = launchServerWithStateMachineEnableSnapshot(group, peers, "n2", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        serverList.add(dLedgerServer0);
        serverList.add(dLedgerServer1);
        serverList.add(dLedgerServer2);
        Thread.sleep(2000);
        // State machine could only be recovered from snapshot due to the entry has been removed after saving snapshot
        for (DLedgerServer server : serverList) {
            assertEquals(98, server.getDLedgerStore().getLedgerEndIndex());
            // check statemachine
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            assertEquals(99, fsm.getTotalEntries());
        }

        // now we append an entry will trigger the snapshotting
        // this time will delete entries on a scale of 90 to 99
        AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[512]);
        assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        assertEquals(99, appendEntryResponse.getIndex());
        Thread.sleep(2000);
        for (DLedgerServer server : serverList) {
            MockStateMachine stateMachine = (MockStateMachine) server.getStateMachine();
            assertEquals(99, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(stateMachine.getLastSnapshotIncludedIndex(), server.getDLedgerStore().getLedgerBeforeBeginIndex());
            // check statemachine
            assertEquals(100, stateMachine.getTotalEntries());
        }

        Thread.sleep(100);
        // Shutdown server
        dLedgerServer0.shutdown();
        dLedgerServer1.shutdown();
        dLedgerServer2.shutdown();
        serverList.clear();
        // Restart server and apply snapshot
        dLedgerServer0 = launchServerWithStateMachineEnableSnapshot(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        dLedgerServer1 = launchServerWithStateMachineEnableSnapshot(group, peers, "n1", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        dLedgerServer2 = launchServerWithStateMachineEnableSnapshot(group, peers, "n2", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        serverList.add(dLedgerServer0);
        serverList.add(dLedgerServer1);
        serverList.add(dLedgerServer2);
        Thread.sleep(2000);
        // State machine could only be recovered from snapshot due to the entry has been removed after saving snapshot
        for (DLedgerServer server : serverList) {
            assertEquals(99, server.getDLedgerStore().getLedgerEndIndex());
            // check statemachine
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            assertEquals(100, fsm.getTotalEntries());
        }
    }

    @Test
    public void testInstallSnapshot() throws Exception {
        // Launch server
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerWithStateMachineEnableSnapshot(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        DLedgerServer dLedgerServer1 = launchServerWithStateMachineEnableSnapshot(group, peers, "n1", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        // DLedgerServer dLedgerServer2 = launchServerWithStateMachineEnableSnapshot(group, peers, "n2", "n1", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>();
        serverList.add(dLedgerServer0);
        serverList.add(dLedgerServer1);
        // Launch client
        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        // append 99 entries, each 10 entries will trigger one snapshotting
        for (int i = 0; i < 99; i++) {
            if (i % 10 == 0) {
                Thread.sleep(200);
            }
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[512]);
            assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(2000);
        for (DLedgerServer server : serverList) {
            MockStateMachine stateMachine = (MockStateMachine) server.getStateMachine();
            assertEquals(98, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(stateMachine.getLastSnapshotIncludedIndex(), server.getDLedgerStore().getLedgerBeforeBeginIndex());
            // check statemachine
            assertEquals(99, stateMachine.getTotalEntries());
        }

        // now we append an entry will trigger the snapshotting
        // this time will delete entries on a scale of 90 to 99
        AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[512]);
        assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        assertEquals(99, appendEntryResponse.getIndex());
        Thread.sleep(2000);
        for (DLedgerServer server : serverList) {
            MockStateMachine stateMachine = (MockStateMachine) server.getStateMachine();
            assertEquals(99, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(stateMachine.getLastSnapshotIncludedIndex(), server.getDLedgerStore().getLedgerBeforeBeginIndex());
            // check statemachine
            assertEquals(100, stateMachine.getTotalEntries());
        }

        Thread.sleep(100);
        // Shutdown server
        dLedgerServer0.shutdown();
        dLedgerServer1.shutdown();
        serverList.clear();
        // Restart server0 and server2 and apply snapshot, server1 simulated offline
        dLedgerServer0 = launchServerWithStateMachineEnableSnapshot(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        DLedgerServer dLedgerServer2 = launchServerWithStateMachineEnableSnapshot(group, peers, "n2", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        serverList.add(dLedgerServer0);
        serverList.add(dLedgerServer2);
        Thread.sleep(2000);
        // State machine could only be recovered from snapshot due to the entry has been removed after saving snapshot
        for (DLedgerServer server : serverList) {
            assertEquals(99, server.getDLedgerStore().getLedgerEndIndex());
            // check statemachine
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            assertEquals(100, fsm.getTotalEntries());
        }

        // now keep appending entries
        for (int i = 100; i < 200; i++) {
            appendEntryResponse = dLedgerClient.append(new byte[512]);
            assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(2000);
        for (DLedgerServer server : serverList) {
            MockStateMachine stateMachine = (MockStateMachine) server.getStateMachine();
            assertEquals(199, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(stateMachine.getLastSnapshotIncludedIndex(), server.getDLedgerStore().getLedgerBeforeBeginIndex());
            // check statemachine
            assertEquals(200, stateMachine.getTotalEntries());
        }
        // shutdown server0 and server2
        dLedgerServer0.shutdown();
        dLedgerServer2.shutdown();
        serverList.clear();
        // restart all three servers, verify snapshot loading and verify if server1 catch up with cluster
        dLedgerServer0 = launchServerWithStateMachineEnableSnapshot(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        dLedgerServer1 = launchServerWithStateMachineEnableSnapshot(group, peers, "n1", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        dLedgerServer2 = launchServerWithStateMachineEnableSnapshot(group, peers, "n2", "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());
        serverList.add(dLedgerServer0);
        serverList.add(dLedgerServer1);
        serverList.add(dLedgerServer2);
        Thread.sleep(2000);
        for (DLedgerServer server : serverList) {
            assertEquals(199, server.getDLedgerStore().getLedgerEndIndex());
            // check statemachine
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            assertEquals(200, fsm.getTotalEntries());
        }
    }

    @Test
    public void testSnapshotReservedNum() throws InterruptedException {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("%s-localhost:%d", selfId, nextPort());
        DLedgerServer server = launchServerWithStateMachineEnableSnapshot(group, peers, selfId, "n0", DLedgerConfig.FILE, 10, 1024, new MockStateMachine());

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
        assertEquals(snapshotCnt, maxSnapshotReservedNum);
    }

    @Test
    public void testLoadErrorSnapshot() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("%s-localhost:%d", selfId, nextPort());
        String snapshotBaseDirPrefix = STORE_PATH + File.separator + group + File.separator + "dledger-" +
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

        DLedgerServer server = launchServerWithStateMachineEnableSnapshot(group, peers, "n0", "n0", DLedgerConfig.FILE, 10, 10 * 1024 * 1024, new MockStateMachine());
        Thread.sleep(1000);

        StateMachineCaller caller = server.getFsmCaller();
        MockStateMachine fsm = (MockStateMachine) caller.getStateMachine();
        MemberState memberState = server.getMemberState();
        assertEquals(memberState.getAppliedIndex(), 8);
        assertEquals(fsm.getTotalEntries(), 80);
        caller.shutdown();
    }
}
