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
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AppendAndGetTest extends ServerTestHarness {

    @Test
    public void testSingleServerInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10001";
        launchServer(group, peers, selfId, selfId, DLedgerConfig.MEMORY);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloSingleServerInMemory" + i).getBytes());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(("HelloSingleServerInMemory" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testSingleServerInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10002";
        launchServer(group, peers, selfId, selfId, DLedgerConfig.FILE);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        long expectedPos = 0L;
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(new byte[100]);
            Assertions.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
            Assertions.assertEquals(expectedPos, appendEntryResponse.getPos());
            expectedPos = expectedPos + DLedgerEntry.BODY_OFFSET + 100;
        }
        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(new byte[100], getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThreeServerInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.MEMORY);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.MEMORY);
        DLedgerClient dLedgerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(1000);
        Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(("HelloThreeServerInMemory" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThreeServerInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10006;n1-localhost:10007;n2-localhost:10008";
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.FILE);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("HelloThreeServerInFile" + i).getBytes());
            Assertions.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assertions.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(100);
        Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(("HelloThreeServerInFile" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThreeServerInFileWithAsyncRequests() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", "n1", DLedgerConfig.FILE);
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(dLedgerServer1.getMemberState().getSelfId());
            request.setBody(("testThreeServerInFileWithAsyncRequests" + i).getBytes());
            futures.add(dLedgerServer1.handleAppend(request));
        }
        Thread.sleep(1000);
        Assertions.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assertions.assertTrue(future.isDone());
            Assertions.assertEquals(i, future.get().getIndex());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), future.get().getCode());

            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            DLedgerEntry entry = getEntriesResponse.getEntries().get(0);
            Assertions.assertEquals(1, getEntriesResponse.getEntries().size());
            Assertions.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assertions.assertArrayEquals(("testThreeServerInFileWithAsyncRequests" + i).getBytes(), entry.getBody());
            //assert the pos
            Assertions.assertEquals(entry.getPos(), future.get().getPos());
        }
    }
}
