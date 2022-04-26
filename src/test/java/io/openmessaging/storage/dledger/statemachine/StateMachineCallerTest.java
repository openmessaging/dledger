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

package io.openmessaging.storage.dledger.statemachine;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.utils.Pair;

import static org.junit.jupiter.api.Assertions.*;

class StateMachineCallerTest extends ServerTestHarness {

    public Pair<StateMachineCaller, MockStateMachine> mockCaller() {
        DLedgerConfig config = new DLedgerConfig();
        MemberState memberState = new MemberState(config);
        memberState.changeToLeader(0);
        final DLedgerMemoryStore dLedgerMemoryStore = new DLedgerMemoryStore(config, memberState);
        for (int i = 0; i < 10; i++) {
            final DLedgerEntry entry = new DLedgerEntry();
            entry.setIndex(i);
            entry.setTerm(0);
            dLedgerMemoryStore.appendAsLeader(entry);
        }
        final MockStateMachine fsm = new MockStateMachine();
        final StateMachineCaller caller = new StateMachineCaller(dLedgerMemoryStore, fsm, null);
        caller.start();
        return new Pair<>(caller, fsm);
    }

    @Test
    public void testOnCommitted() throws Exception {
        final Pair<StateMachineCaller, MockStateMachine> result = mockCaller();
        final StateMachineCaller caller = result.getKey();
        final MockStateMachine fsm = result.getValue();
        caller.onCommitted(9);
        Thread.sleep(1000);
        assertEquals(fsm.getAppliedIndex(), 9);
        assertEquals(fsm.getTotalEntries(), 10);

        caller.shutdown();
    }

    @Test
    public void testOnCommittedWithServer() throws InterruptedException {
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
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1000);
        for (DLedgerServer server : serverList) {
            Assertions.assertEquals(9, server.getdLedgerStore().getLedgerEndIndex());
        }

        // Check statemachine
        for (DLedgerServer server : serverList) {
            final MockStateMachine fsm = (MockStateMachine) server.getStateMachine();
            Assertions.assertEquals(9, fsm.getAppliedIndex());
            Assertions.assertEquals(10, fsm.getTotalEntries());
        }
    }
}