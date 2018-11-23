/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dleger;

import io.openmessaging.storage.dleger.client.DLegerClient;
import io.openmessaging.storage.dleger.entry.DLegerEntry;
import io.openmessaging.storage.dleger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;
import io.openmessaging.storage.dleger.protocol.GetEntriesResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Test;

public class AppendAndGetTest extends ServerTestHarness {

    @Test
    public void testSingleServerInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10001";
        launchServer(group, peers, selfId, selfId, DLegerConfig.MEMORY);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLegerClient.append(("HelloSingleServerInMemory" + i).getBytes());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloSingleServerInMemory" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testSingleServerInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:10002";
        launchServer(group, peers, selfId, selfId, DLegerConfig.FILE);
        DLegerClient dLegerClient = launchClient(group, peers);
        long expectedPos = 0L;
        for (long i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLegerClient.append(new byte[100]);
            Assert.assertEquals(appendEntryResponse.getCode(), DLegerResponseCode.SUCCESS.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
            Assert.assertEquals(expectedPos, appendEntryResponse.getPos());
            expectedPos = expectedPos + DLegerEntry.BODY_OFFSET + 100;
        }
        for (long i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(new byte[100], getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThreeServerInMemory() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n1", DLegerConfig.MEMORY);
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n1", DLegerConfig.MEMORY);
        DLegerServer dLegerServer2 = launchServer(group, peers, "n2", "n1", DLegerConfig.MEMORY);
        DLegerClient dLegerClient = launchClient(group, peers.split(";")[0]);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLegerClient.append(("HelloThreeServerInMemory" + i).getBytes());
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), appendEntryResponse.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(100);
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(9, dLegerServer1.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(9, dLegerServer2.getdLegerStore().getLegerEndIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloThreeServerInMemory" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThressServerInFile() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = "n0-localhost:10006;n1-localhost:10007;n2-localhost:10008";
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer2 = launchServer(group, peers, "n2", "n1", DLegerConfig.FILE);
        DLegerClient dLegerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLegerClient.append(("HelloThreeServerInFile" + i).getBytes());
            Assert.assertEquals(appendEntryResponse.getCode(), DLegerResponseCode.SUCCESS.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(100);
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(9, dLegerServer1.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(9, dLegerServer2.getdLegerStore().getLegerEndIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("HelloThreeServerInFile" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testThreeServerInFileWithAsyncRequests() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n1", DLegerConfig.FILE);
        DLegerServer dLegerServer2 = launchServer(group, peers, "n2", "n1", DLegerConfig.FILE);
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(dLegerServer1.getMemberState().getSelfId());
            request.setBody(("testThreeServerInFileWithAsyncRequests" + i).getBytes());
            futures.add(dLegerServer1.handleAppend(request));
        }
        Thread.sleep(500);
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(9, dLegerServer1.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(9, dLegerServer2.getdLegerStore().getLegerEndIndex());

        DLegerClient dLegerClient = launchClient(group, peers);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assert.assertTrue(future.isDone());
            Assert.assertEquals(i, future.get().getIndex());
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), future.get().getCode());

            GetEntriesResponse getEntriesResponse = dLegerClient.get(i);
            DLegerEntry entry = getEntriesResponse.getEntries().get(0);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("testThreeServerInFileWithAsyncRequests" + i).getBytes(), entry.getBody());
            //assert the pos
            Assert.assertEquals(entry.getPos(), future.get().getPos());
        }
    }
}
