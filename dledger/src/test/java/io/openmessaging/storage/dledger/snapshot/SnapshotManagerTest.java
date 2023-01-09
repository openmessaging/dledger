package io.openmessaging.storage.dledger.snapshot;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
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


    @Test
    public void testSaveAndLoadSnapshot() throws InterruptedException {
        // Launch server
        String group = UUID.randomUUID().toString();
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
        // append 99 entries, each 10 entries will trigger one snapshotting
        for (int i = 0; i < 99; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[512]);
            assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(2000);
        for (DLedgerServer server : serverList) {
            assertEquals(98, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(89, server.getDLedgerStore().getLedgerBeforeBeginIndex());
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
            assertEquals(99, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(99, server.getDLedgerStore().getLedgerBeforeBeginIndex());
            // check statemachine
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
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
        Thread.sleep(2000);
        // State machine could only be recovered from snapshot due to the entry has been removed after saving snapshot
        for (DLedgerServer server : serverList) {
            assertEquals(99, server.getDLedgerStore().getLedgerEndIndex());
            assertEquals(99, server.getDLedgerStore().getLedgerBeforeBeginIndex());
            // check statemachine
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            assertEquals(100, fsm.getTotalEntries());
        }
    }

    @Test
    public void testSnapshotReservedNum() throws InterruptedException {
        String group = UUID.randomUUID().toString();
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
        assertEquals(snapshotCnt, maxSnapshotReservedNum);
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
