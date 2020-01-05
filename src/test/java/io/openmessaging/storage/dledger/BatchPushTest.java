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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class BatchPushTest extends ServerTestHarness{
    @Test
    public void testBatchPushWithOneByOneRequests() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerEnableBatchPush(group, peers, "n0", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer1 = launchServerEnableBatchPush(group, peers, "n1", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer2 = launchServerEnableBatchPush(group, peers, "n2", "n1", DLedgerConfig.FILE);
        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < 10; i++) {
            AppendEntryResponse appendEntryResponse = dLedgerClient.append(("testBulkCopyWithOneByOneRequests" + i).getBytes());
            Assert.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Thread.sleep(100);
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        for (int i = 0; i < 10; i++) {
            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("testBulkCopyWithOneByOneRequests" + i).getBytes(), getEntriesResponse.getEntries().get(0).getBody());
        }
    }

    @Test
    public void testBatchPushWithAsyncRequests() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerEnableBatchPush(group, peers, "n0", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer1 = launchServerEnableBatchPush(group, peers, "n1", "n1", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer2 = launchServerEnableBatchPush(group, peers, "n2", "n1", DLedgerConfig.FILE);
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(dLedgerServer1.getMemberState().getSelfId());
            request.setBody(("testBatchPushWithAsyncRequests" + i).getBytes());
            futures.add(dLedgerServer1.handleAppend(request));
        }
        Thread.sleep(500);
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(9, dLedgerServer2.getdLedgerStore().getLedgerEndIndex());

        DLedgerClient dLedgerClient = launchClient(group, peers);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assert.assertTrue(future.isDone());
            Assert.assertEquals(i, future.get().getIndex());
            Assert.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), future.get().getCode());

            GetEntriesResponse getEntriesResponse = dLedgerClient.get(i);
            DLedgerEntry entry = getEntriesResponse.getEntries().get(0);
            Assert.assertEquals(1, getEntriesResponse.getEntries().size());
            Assert.assertEquals(i, getEntriesResponse.getEntries().get(0).getIndex());
            Assert.assertArrayEquals(("testBatchPushWithAsyncRequests" + i).getBytes(), entry.getBody());
            //assert the pos
            Assert.assertEquals(entry.getPos(), future.get().getPos());
        }
    }

    @Test
    public void testBatchPushNetworkOffline() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLedgerServer dLedgerServer0 = launchServerEnableBatchPush(group, peers, "n0", "n0", DLedgerConfig.FILE);
        List<CompletableFuture<AppendEntryResponse>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[128]);
            CompletableFuture<AppendEntryResponse> future = dLedgerServer0.handleAppend(appendEntryRequest);
            Assert.assertTrue(future instanceof AppendFuture);
            futures.add(future);
        }
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Thread.sleep(dLedgerServer0.getdLedgerConfig().getMaxWaitAckTimeMs() + 100);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assert.assertTrue(future.isDone());
            Assert.assertEquals(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode(), future.get().getCode());
        }

        boolean hasWait = false;
        for (int i = 0; i < dLedgerServer0.getdLedgerConfig().getMaxPendingRequestsNum(); i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[128]);
            CompletableFuture<AppendEntryResponse> future = dLedgerServer0.handleAppend(appendEntryRequest);
            Assert.assertTrue(future instanceof AppendFuture);
            if (future.isDone()) {
                Assert.assertEquals(DLedgerResponseCode.LEADER_PENDING_FULL.getCode(), future.get().getCode());
                hasWait = true;
                break;
            }
        }
        Assert.assertTrue(hasWait);
    }

    @Test
    public void testBatchPushNetworkNotStable() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLedgerServer dLedgerServer0 = launchServerEnableBatchPush(group, peers, "n0", "n0", DLedgerConfig.FILE);
        AtomicBoolean sendSuccess = new AtomicBoolean(false);
        AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
        appendEntryRequest.setGroup(group);
        appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
        appendEntryRequest.setBody(new byte[128]);
        CompletableFuture<AppendEntryResponse> future = dLedgerServer0.handleAppend(appendEntryRequest);
        Assert.assertTrue(future instanceof AppendFuture);
        future.whenComplete((x, ex) -> {
            sendSuccess.set(true);
        });
        Thread.sleep(500);
        Assert.assertTrue(!sendSuccess.get());
        //start server1
        DLedgerServer dLedgerServer1 = launchServerEnableBatchPush(group, peers, "n1", "n0", DLedgerConfig.FILE);
        Thread.sleep(1500);
        Assert.assertTrue(sendSuccess.get());
        //shutdown server1
        dLedgerServer1.shutdown();
        sendSuccess.set(false);
        future = dLedgerServer0.handleAppend(appendEntryRequest);
        Assert.assertTrue(future instanceof AppendFuture);
        future.whenComplete((x, ex) -> {
            sendSuccess.set(true);
        });
        Thread.sleep(500);
        Assert.assertTrue(!sendSuccess.get());
        //restart servre1
        dLedgerServer1 = launchServerEnableBatchPush(group, peers, "n1", "n0", DLedgerConfig.FILE);
        Thread.sleep(1500);
        Assert.assertTrue(sendSuccess.get());

        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(1, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(0, dLedgerServer1.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(1, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
    }

    @Test
    public void testBatchPushMissed() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerEnableBatchPush(group, peers, "n0", "n0", DLedgerConfig.FILE);
        DLedgerServer dLedgerServer1 = launchServerEnableBatchPush(group, peers, "n1", "n0", DLedgerConfig.FILE);
        DLedgerServer mockServer1 = Mockito.spy(dLedgerServer1);
        AtomicInteger callNum = new AtomicInteger(0);
        doAnswer(x -> {
            if (callNum.incrementAndGet() % 3 == 0) {
                return new CompletableFuture<>();
            } else {
                return dLedgerServer1.handlePush(x.getArgument(0));
            }
        }).when(mockServer1).handlePush(any());
        ((DLedgerRpcNettyService) dLedgerServer1.getdLedgerRpcService()).setdLedgerServer(mockServer1);

        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setBody(new byte[128]);
            appendEntryRequest.setRemoteId(dLedgerServer0.getMemberState().getSelfId());
            AppendEntryResponse appendEntryResponse = dLedgerServer0.handleAppend(appendEntryRequest).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(appendEntryResponse.getCode(), DLedgerResponseCode.SUCCESS.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());

        Assert.assertEquals(0, dLedgerServer1.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(9, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
    }

    @Test
    public void testBatchPushTruncate() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServerEnableBatchPush(group, peers, "n0", "n0", DLedgerConfig.FILE);
        for (int i = 0; i < 10; i++) {
            DLedgerEntry entry = new DLedgerEntry();
            entry.setBody(new byte[128]);
            DLedgerEntry resEntry = dLedgerServer0.getdLedgerStore().appendAsLeader(entry);
            Assert.assertEquals(i, resEntry.getIndex());
        }
        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(9, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        List<DLedgerEntry> entries = new ArrayList<>();
        for (long i = 0; i < 10; i++) {
            entries.add(dLedgerServer0.getdLedgerStore().get(i));
        }
        dLedgerServer0.shutdown();

        DLedgerServer dLedgerServer1 = launchServerEnableBatchPush(group, peers, "n1", "n0", DLedgerConfig.FILE);
        for (int i = 0; i < 5; i++) {
            DLedgerEntry resEntry = dLedgerServer1.getdLedgerStore().appendAsFollower(entries.get(i), 0, "n0");
            Assert.assertEquals(i, resEntry.getIndex());
        }
        dLedgerServer1.shutdown();

        //change leader from n0 => n1
        dLedgerServer1 = launchServerEnableBatchPush(group, peers, "n1", "n1", DLedgerConfig.FILE);
        dLedgerServer0 = launchServerEnableBatchPush(group, peers, "n0", "n1", DLedgerConfig.FILE);
        Thread.sleep(1000);
        Assert.assertEquals(0, dLedgerServer0.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(4, dLedgerServer0.getdLedgerStore().getLedgerEndIndex());
        Assert.assertEquals(0, dLedgerServer1.getdLedgerStore().getLedgerBeginIndex());
        Assert.assertEquals(4, dLedgerServer1.getdLedgerStore().getLedgerEndIndex());
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest request = new AppendEntryRequest();
            request.setGroup(group);
            request.setRemoteId(dLedgerServer1.getMemberState().getSelfId());
            request.setBody(new byte[128]);
            long appendIndex = dLedgerServer1.handleAppend(request).get().getIndex();
            Assert.assertEquals(i + 5, appendIndex);
        }
    }
}
