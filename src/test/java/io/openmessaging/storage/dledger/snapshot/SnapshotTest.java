/*
 * Copyright 2017-2020 the original author or authors.
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

package io.openmessaging.storage.dledger.snapshot;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.ServerTestHarness;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.utils.Pair;

import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriterImpl;

import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotReaderImpl;
import io.openmessaging.storage.dledger.statemachine.MockStateMachine;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotTest extends ServerTestHarness {

    @Test
    public void testWriteReadMeta() throws Exception {
        SnapshotWriterImpl writer = new SnapshotWriterImpl(1, 2);
        writer.writeMeta();
        SnapshotReaderImpl reader = new SnapshotReaderImpl();
        
        assertEquals(reader.getLastIncludedIndex(), 1);
    }

    @Test
    public void testWriteReadData() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.MEMORY);
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>() {
            {
                add(dLedgerServer0);
                add(dLedgerServer1);
                add(dLedgerServer2);
            }
        };
        // Register state machine
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = new MockStateMachine();
            server.registerStateMachine(fsm);
        }

        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 9; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1000);
        // using Reader to read the data and check if it is equal to fsm's logs
        for (DLedgerServer server : serverList) {
            MockStateMachine mocksm = (MockStateMachine)server.getStateMachine();
            List<ByteBuffer> logs_now = mocksm.getLogs();
            // read from snapshot
            SnapshotReaderImpl reader = new SnapshotReaderImpl();
            mocksm.onSnapshotLoad(reader);
            Assertions.assertEquals(6, mocksm.getLogs().size());
            Assertions.assertEquals(logs_now, mocksm.getLogs());
            
        }

    }

    @Test
    public void testRecoveryFromSnapshot() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.MEMORY);
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>() {
            {
                add(dLedgerServer0);
                add(dLedgerServer1);
                add(dLedgerServer2);
            }
        };
        // Register state machine
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = new MockStateMachine();
            server.registerStateMachine(fsm);
        }

        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 9; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1000);
        // using Reader to read the data and check if it is equal to fsm's logs

            MockStateMachine mocksm = (MockStateMachine)dLedgerServer1.getStateMachine();

            // read from snapshot
            dLedgerServer0.shutdown();
            dLedgerServer2.shutdown();
            ((DLedgerMemoryStore)(dLedgerServer1.getDLedgerStore())).snapshotHandle();
            
            Thread.sleep(5000);
            MockStateMachine mocksmAfterRecovery = (MockStateMachine)dLedgerServer1.getStateMachine();
            Assertions.assertEquals(6, mocksmAfterRecovery.getLogs().size());
            Assertions.assertEquals(mocksm.getLogs(), mocksmAfterRecovery.getLogs());
            
        ;
    }

    @Test
    public void testCleanLogs() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.MEMORY);
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>() {
            {
                add(dLedgerServer0);
                add(dLedgerServer1);
                add(dLedgerServer2);
            }
        };
        // Register state machine
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = new MockStateMachine();
            server.registerStateMachine(fsm);
        }

        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 7; i++) {
            System.out.println("server1:"+dLedgerServer1.getDLedgerStore().getDataSize());
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1000);
        // using Reader to read the data and check if it is equal to fsm's logs
        for (DLedgerServer server : serverList) {
            MockStateMachine mocksm = (MockStateMachine)server.getStateMachine();
            System.out.println(server.getDLedgerStore().getDataSize());
            List<ByteBuffer> logs_now = mocksm.getLogs();
            // read from snapshot
            SnapshotReaderImpl reader = new SnapshotReaderImpl();
            mocksm.onSnapshotLoad(reader);
            Assertions.assertEquals(6, mocksm.getLogs().size());
            Assertions.assertEquals(logs_now, mocksm.getLogs());
            
        }
    }

    @Test
    public void testRocketmqController() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.MEMORY);
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>() {
            {
                add(dLedgerServer0);
                add(dLedgerServer1);
                add(dLedgerServer2);
            }
        };
        // Register state machine
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = new MockStateMachine();
            server.registerStateMachine(fsm);
        }

        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 9; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1000);
        // using Reader to read the data and check if it is equal to fsm's logs
        for (DLedgerServer server : serverList) {
            MockStateMachine mocksm = (MockStateMachine)server.getStateMachine();
            List<ByteBuffer> logs_now = mocksm.getLogs();
            // read from snapshot
            SnapshotReaderImpl reader = new SnapshotReaderImpl();
            mocksm.onSnapshotLoad(reader);
            Assertions.assertEquals(6, mocksm.getLogs().size());
            Assertions.assertEquals(logs_now, mocksm.getLogs());
            
        }
    }
}