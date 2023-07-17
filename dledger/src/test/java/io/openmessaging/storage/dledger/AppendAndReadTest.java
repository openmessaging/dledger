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
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.statemachine.register.RegisterReadProcessor;
import io.openmessaging.storage.dledger.statemachine.register.RegisterReadRequest;
import io.openmessaging.storage.dledger.statemachine.register.RegisterReadResponse;
import io.openmessaging.storage.dledger.statemachine.register.RegisterStateMachine;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import io.openmessaging.storage.dledger.utils.BytesUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

public class AppendAndReadTest extends ServerTestHarness {

    public static final String STORE_PATH = FileTestUtil.createTestDir("AppendAndReadTest");

    @Override
    protected String getBaseDir() {
        return STORE_PATH;
    }
    @Test
    public void testSingleServerInMemory() {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerServer dLedgerServer = launchServerWithStateMachineDisableSnapshot(group, peers, selfId, selfId, DLedgerConfig.MEMORY,
            102400, new RegisterStateMachine());
        dLedgerServer.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer)));

        appendAndRead(group, peers);
    }

    @Test
    public void testSingleServerInFile() {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("n0-localhost:%d", nextPort());
        DLedgerServer dLedgerServer = launchServerWithStateMachineDisableSnapshot(group, peers, selfId, selfId, DLedgerConfig.FILE,
            102400, new RegisterStateMachine());
        dLedgerServer.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer)));

        appendAndRead(group, peers);
    }

    @Test
    public void testThreeServerInMemory() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerWithStateMachineDisableSnapshot(group, peers, "n0", "n1", DLedgerConfig.MEMORY, 102400, new RegisterStateMachine());
        DLedgerServer dLedgerServer1 = launchServerWithStateMachineDisableSnapshot(group, peers, "n1", "n1", DLedgerConfig.MEMORY, 102400, new RegisterStateMachine());
        DLedgerServer dLedgerServer2 = launchServerWithStateMachineDisableSnapshot(group, peers, "n2", "n1", DLedgerConfig.MEMORY, 102400, new RegisterStateMachine());
        dLedgerServer0.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer0)));
        dLedgerServer1.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer1)));
        dLedgerServer2.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer2)));

        appendAndRead(group, peers.split(";")[1]);
    }

    @Test
    public void testThreeServerInFile() {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerWithStateMachineDisableSnapshot(group, peers, "n0", "n1", DLedgerConfig.FILE, 102400, new RegisterStateMachine());
        DLedgerServer dLedgerServer1 = launchServerWithStateMachineDisableSnapshot(group, peers, "n1", "n1", DLedgerConfig.FILE, 102400, new RegisterStateMachine());
        DLedgerServer dLedgerServer2 = launchServerWithStateMachineDisableSnapshot(group, peers, "n2", "n1", DLedgerConfig.FILE, 102400, new RegisterStateMachine());
        dLedgerServer0.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer0)));
        dLedgerServer1.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer1)));
        dLedgerServer2.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer2)));

        appendAndRead(group, peers.split(";")[1]);
    }

    private void appendAndRead(String group, String peers) {
        DLedgerClient dLedgerClient = launchClient(group, peers);

        for (int i = 1; i <= 10; i++) {
            RegisterReadResponse resp = readKeyValue(i, dLedgerClient);
            Assertions.assertEquals(i, resp.getKey());
            Assertions.assertEquals(-1, resp.getValue());
        }

        for (int i = 1; i <= 10; i++) {
            AppendEntryResponse appendEntryResponse = appendKeyValue(i, 10 * i, dLedgerClient);
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
        for (int i = 1; i <= 10; i++) {
            RegisterReadResponse resp = readKeyValue(i, dLedgerClient);
            Assertions.assertEquals(i, resp.getKey());
            Assertions.assertEquals(i * 10, resp.getValue());
        }
        for (int i = 1; i <= 10; i++) {
            AppendEntryResponse appendEntryResponse = appendKeyValue(i, 20 * i, dLedgerClient);
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
        }
        for (int i = 1; i <= 10; i++) {
            RegisterReadResponse resp = readKeyValue(i, dLedgerClient);
            Assertions.assertEquals(i, resp.getKey());
            Assertions.assertEquals(i * 20, resp.getValue());
        }
    }

    private AppendEntryResponse appendKeyValue(int key, int value, DLedgerClient client) {
        byte[] keyBytes = BytesUtil.intToBytes(key);
        byte[] valueBytes = BytesUtil.intToBytes(value);
        byte[] afterCodingBytes = new byte[8];
        System.arraycopy(keyBytes, 0, afterCodingBytes, 0, 4);
        System.arraycopy(valueBytes, 0, afterCodingBytes, 4, 4);
        return client.append(afterCodingBytes);
    }

    private RegisterReadResponse readKeyValue(int key, DLedgerClient client) {
        RegisterReadRequest registerReadRequest = new RegisterReadRequest(key);
        RegisterReadResponse response = client.invokeUserDefineRequest(registerReadRequest, RegisterReadResponse.class, true);
        Assertions.assertNotNull(response);
        return response;
    }
}
