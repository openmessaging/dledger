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
import io.openmessaging.storage.dledger.protocol.RequestOrResponse;
import io.openmessaging.storage.dledger.statemachine.register.RegisterReadProcessor;
import io.openmessaging.storage.dledger.statemachine.register.RegisterReadRequest;
import io.openmessaging.storage.dledger.statemachine.register.RegisterReadResponse;
import io.openmessaging.storage.dledger.statemachine.register.RegisterStateMachine;
import io.openmessaging.storage.dledger.util.BytesUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;

public class AppendAndReadTest extends ServerTestHarness {

    @Test
    public void testSingleServerInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:11001";
        DLedgerServer dLedgerServer = launchServerWithStateMachine(group, peers, selfId, selfId, DLedgerConfig.MEMORY,
                100000, 102400, new RegisterStateMachine());
        dLedgerServer.registerUserDefineProcessors(Collections.singletonList(new RegisterReadProcessor(dLedgerServer)));

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
        RegisterReadResponse response = client.invokeUserDefineRequest(registerReadRequest, RegisterReadResponse.class,true);
        Assertions.assertNotNull(response);
        return response;
    }
}
