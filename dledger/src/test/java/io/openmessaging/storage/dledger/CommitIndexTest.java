/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.awaitility.Awaitility.await;

public class CommitIndexTest extends ServerTestHarness {

    public static final String STORE_PATH = FileTestUtil.createTestDir("CommitIndexTest");

    @Override
    protected String getBaseDir() {
        return STORE_PATH;
    }

    @Test
    public void testDisableFastAdvanceCommitIndex() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer server0 = launchServer(group, peers, "n0", "n0");
        DLedgerServer server1 = launchServer(group, peers, "n1", "n0");
        DLedgerServer server2 = launchServer(group, peers, "n2", "n0");
        await().atMost(6, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> {
            return server0.isLeader();
        });
        // at beginning, the commit index is -1
        Assertions.assertEquals(-1, server0.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server2.getMemberState().getCommittedIndex());

        // mock that n1 and n2 all receive push request from n0, but they don't return response
        // so n0 can't commit the index(0) although it has been replicated to n1 and n2
        DLedgerServer mockServer1 = Mockito.spy(server1);
        DLedgerServer mockServer2 = Mockito.spy(server2);
        AtomicBoolean hasPushed1 = new AtomicBoolean(false);
        AtomicBoolean hasPushed2 = new AtomicBoolean(false);
        Mockito.doAnswer((ink) -> {
            PushEntryRequest request = ink.getArgument(0);
            if (hasPushed1.compareAndSet(false, true)) {
                server1.handlePush(request);
            }
            return new CompletableFuture<>();
        }).when(mockServer1).handlePush(ArgumentMatchers.argThat((request) -> {
            return request.getType() == PushEntryRequest.Type.APPEND && request.getFirstEntryIndex() == 0 && request.getLocalId().equals("n0");
        }));
        Mockito.doAnswer((ink) -> {
            PushEntryRequest request = ink.getArgument(0);
            if (hasPushed2.compareAndSet(false, true)) {
                server2.handlePush(request);
            }
            return new CompletableFuture<>();
        }).when(mockServer2).handlePush(ArgumentMatchers.argThat((request) -> {
            return request.getType() == PushEntryRequest.Type.APPEND && request.getFirstEntryIndex() == 0 && request.getLocalId().equals("n0");
        }));
        ((DLedgerRpcNettyService) server1.getDLedgerRpcService()).setDLedger(mockServer1);
        ((DLedgerRpcNettyService) server2.getDLedgerRpcService()).setDLedger(mockServer2);
        // send append request to n0
        AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
        appendEntryRequest.setGroup(group);
        appendEntryRequest.setBody(new byte[128]);
        appendEntryRequest.setRemoteId(server0.getMemberState().getSelfId());
        AppendEntryResponse resp = server0.handleAppend(appendEntryRequest).get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode(), resp.getCode());
        // n0, n1 ,n2 all have the same commit index -1
        Assertions.assertEquals(-1, server0.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server2.getMemberState().getCommittedIndex());
        // but all of them have the entry-0
        Assertions.assertEquals(0, server0.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(0, server1.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(0, server2.getDLedgerStore().getLedgerEndIndex());
        // now n0 shutdown
        server0.shutdown();
        // wait n1 or n2 become leader
        AtomicReference<DLedgerServer> leaderRef = new AtomicReference<>(null);
        await().atMost(6, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> {
            if (mockServer1.isLeader()) {
                leaderRef.set(mockServer1);
                return true;
            }
            if (mockServer2.isLeader()) {
                leaderRef.set(mockServer2);
                return true;
            }
            return false;
        });
        DLedgerServer leader = leaderRef.get();
        Assertions.assertNotNull(leader);
        Assertions.assertTrue(leader.isLeader());
        // wait 2s, n1 and n2 are still keep the same commit index -1 (leader only can commit the entry which in current term)
        Thread.sleep(2000);
        Assertions.assertEquals(-1, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server2.getMemberState().getCommittedIndex());

        // now append a new entry to n1 and n2, expect the commit index is advanced to 1
        appendEntryRequest.setRemoteId(leader.getMemberState().getSelfId());
        AppendEntryResponse response = leader.handleAppend(appendEntryRequest).get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), response.getCode());
        Assertions.assertEquals(1, response.getIndex());
        // wait 2s, n1 and n2 have update the commit index to 1
        Thread.sleep(2000);
        Assertions.assertEquals(1, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(1, server2.getMemberState().getCommittedIndex());

        // now restart n0
        DLedgerServer newServer0 = launchServer(group, peers, "n0", "n0");
        await().atMost(6, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> {
            return newServer0.isLeader();
        });
        Assertions.assertEquals(1, newServer0.getMemberState().getCommittedIndex());
    }

    @Test
    public void testEnableFastAdvanceCommitIndex() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer server0 = launchServer(group, peers, "n0", "n0", true);
        DLedgerServer server1 = launchServer(group, peers, "n1", "n0", true);
        DLedgerServer server2 = launchServer(group, peers, "n2", "n0", true);
        await().atMost(6, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> {
            return server0.isLeader();
        });
        // at beginning, the commit index is -1
        Assertions.assertEquals(-1, server0.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server2.getMemberState().getCommittedIndex());

        // mock that n1 and n2 all receive push request from n0, but they don't return response
        // so n0 can't commit the index(0) although it has been replicated to n1 and n2
        DLedgerServer mockServer1 = Mockito.spy(server1);
        DLedgerServer mockServer2 = Mockito.spy(server2);
        AtomicBoolean hasPushed1 = new AtomicBoolean(false);
        AtomicBoolean hasPushed2 = new AtomicBoolean(false);
        Mockito.doAnswer((ink) -> {
            PushEntryRequest request = ink.getArgument(0);
            if (hasPushed1.compareAndSet(false, true)) {
                server1.handlePush(request);
            }
            return new CompletableFuture<>();
        }).when(mockServer1).handlePush(ArgumentMatchers.argThat((request) -> {
            return request.getType() == PushEntryRequest.Type.APPEND && request.getFirstEntryIndex() == 0 && request.getLocalId().equals("n0");
        }));
        Mockito.doAnswer((ink) -> {
            PushEntryRequest request = ink.getArgument(0);
            if (hasPushed2.compareAndSet(false, true)) {
                server2.handlePush(request);
            }
            return new CompletableFuture<>();
        }).when(mockServer2).handlePush(ArgumentMatchers.argThat((request) -> {
            return request.getType() == PushEntryRequest.Type.APPEND && request.getFirstEntryIndex() == 0 && request.getLocalId().equals("n0");
        }));
        ((DLedgerRpcNettyService) server1.getDLedgerRpcService()).setDLedger(mockServer1);
        ((DLedgerRpcNettyService) server2.getDLedgerRpcService()).setDLedger(mockServer2);
        // send append request to n0
        AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
        appendEntryRequest.setGroup(group);
        appendEntryRequest.setBody(new byte[128]);
        appendEntryRequest.setRemoteId(server0.getMemberState().getSelfId());
        AppendEntryResponse resp = server0.handleAppend(appendEntryRequest).get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode(), resp.getCode());
        // n0, n1 ,n2 all have the same commit index -1
        Assertions.assertEquals(-1, server0.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(-1, server2.getMemberState().getCommittedIndex());
        // but all of them have the entry-0
        Assertions.assertEquals(0, server0.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(0, server1.getDLedgerStore().getLedgerEndIndex());
        Assertions.assertEquals(0, server2.getDLedgerStore().getLedgerEndIndex());
        // now n0 shutdown
        server0.shutdown();
        // wait n1 or n2 become leader
        AtomicReference<DLedgerServer> leaderRef = new AtomicReference<>(null);
        await().atMost(6, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> {
            if (mockServer1.isLeader()) {
                leaderRef.set(mockServer1);
                return true;
            }
            if (mockServer2.isLeader()) {
                leaderRef.set(mockServer2);
                return true;
            }
            return false;
        });
        DLedgerServer leader = leaderRef.get();
        Assertions.assertNotNull(leader);
        Assertions.assertTrue(leader.isLeader());
        // wait 2s, n1 and n2 will advance the commit index to 1 (entry-0 is the entry we append before, entry-1 is a no-op entry we append when become leader)
        Thread.sleep(2000);
        Assertions.assertEquals(1, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(1, server2.getMemberState().getCommittedIndex());

        // now append a new entry to n1 and n2, expect the commit index is advanced to 2
        appendEntryRequest.setRemoteId(leader.getMemberState().getSelfId());
        AppendEntryResponse response = leader.handleAppend(appendEntryRequest).get(5, TimeUnit.SECONDS);
        Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), response.getCode());
        Assertions.assertEquals(2, response.getIndex());
        // wait 2s, n1 and n2 have update the commit index to 1
        Thread.sleep(2000);
        Assertions.assertEquals(2, server1.getMemberState().getCommittedIndex());
        Assertions.assertEquals(2, server2.getMemberState().getCommittedIndex());

        // now restart n0, expect its commit index is also 2
        // why not 3? because when leader change, n0 hava already advanced the commit index to 2 (commit index is not stale at that time),
        // so it is meaningless to append one more no-op entry to advance the commit index
        DLedgerServer newServer0 = launchServer(group, peers, "n0", "n0", true);
        await().atMost(6, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> {
            return newServer0.isLeader();
        });
        Assertions.assertEquals(2, newServer0.getMemberState().getCommittedIndex());
    }
}
