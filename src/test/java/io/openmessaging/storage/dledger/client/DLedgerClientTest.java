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

package io.openmessaging.storage.dledger.client;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.ServerTestHarness;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DLedgerClientTest extends ServerTestHarness {

    @Test
    void testAppendSingleServer() {

        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10001";
        launchServer(group, peers, selfId, selfId, DLedgerConfig.FILE);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        AppendEntryResponse append = dLedgerClient.append(group.getBytes(StandardCharsets.UTF_8));
        GetEntriesResponse response = dLedgerClient.get(append.getIndex());
        Assertions.assertEquals(group, new String(response.getEntries().get(0).getBody()));
    }

    @Test
    void testAppendThreeServer() throws InterruptedException {

        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10006;n1-localhost:10007;n2-localhost:10008";
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.FILE);
        DLedgerClient dLedgerClient = launchClient(group, peers);

        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append((group + i).getBytes(StandardCharsets.UTF_8));
            Assertions.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }

        Thread.sleep(100);
        Assertions.assertEquals(9, dLedgerServer0.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getDLedgerStore().getLedgerEndIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals((group + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    void testBatchAppendSingleServer() {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10001";
        launchServer(group, peers, selfId, selfId, DLedgerConfig.FILE);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        List<byte[]> bodies = new ArrayList<>();
        bodies.add("1".getBytes(StandardCharsets.UTF_8));
        bodies.add("2".getBytes(StandardCharsets.UTF_8));
        bodies.add("3".getBytes(StandardCharsets.UTF_8));
        dLedgerClient.batchAppend(bodies);
        GetEntriesResponse response0 = dLedgerClient.get(0);
        GetEntriesResponse response1 = dLedgerClient.get(1);
        GetEntriesResponse response2 = dLedgerClient.get(2);
        Assertions.assertEquals("1", new String(response0.getEntries().get(0).getBody()));
        Assertions.assertEquals("2", new String(response1.getEntries().get(0).getBody()));
        Assertions.assertEquals("3", new String(response2.getEntries().get(0).getBody()));
    }

    @Test
    void testBatchAppendThreeServer() throws InterruptedException {

        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10006;n1-localhost:10007;n2-localhost:10008";
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.FILE);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        List<byte[]> bodies = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            bodies.add((group + i).getBytes(StandardCharsets.UTF_8));
        }
        AppendEntryResponse appendEntryResponse = dLedgerClient.batchAppend(bodies);
        Assertions.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
        Assertions.assertEquals(9L, appendEntryResponse.getIndex());
        Thread.sleep(100);
        Assertions.assertEquals(9, dLedgerServer0.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getDLedgerStore().getLedgerEndIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals((group + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }
}