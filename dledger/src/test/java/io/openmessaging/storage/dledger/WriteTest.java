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

import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.common.WriteClosure;
import io.openmessaging.storage.dledger.common.WriteTask;
import io.openmessaging.storage.dledger.statemachine.MockStateMachine;
import io.openmessaging.storage.dledger.util.FileTestUtil;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;

public class WriteTest extends ServerTestHarness {

    public static final String STORE_PATH = FileTestUtil.createTestDir("writeAndReadTest");

    abstract class MockWriteClosure extends WriteClosure<Long> {

        protected Long totalEntries = 0L;

        @Override
        public void setResp(Long aLong) {
            this.totalEntries = aLong;
        }

        @Override
        public Long getResp(Long aLong) {
            return totalEntries;
        }
    }

    @Override
    protected String getBaseDir() {
        return STORE_PATH;
    }

    @Test
    public void testSingleServerInMemory() {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:" + nextPort();
        DLedgerServer dLedgerServer = launchServerWithStateMachineDisableSnapshot(group, peers, selfId, selfId, DLedgerConfig.MEMORY,
            102400, new MockStateMachine());
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            WriteTask task = new WriteTask();
            task.setBody("hello".getBytes());
            final int index = i;
            dLedgerServer.handleWrite(task, new MockWriteClosure() {
                @Override
                public void done(Status status) {
                    Assertions.assertTrue(status.isOk());
                    Assertions.assertEquals(index + 1, totalEntries);
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        }
    }

    @Test
    public void testThreeServerInMemory() {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer1 = launchServerWithStateMachineDisableSnapshot(group, peers, "n0", selfId, DLedgerConfig.MEMORY,
            102400, new MockStateMachine());
        DLedgerServer dLedgerServer2 = launchServerWithStateMachineDisableSnapshot(group, peers, "n1", selfId, DLedgerConfig.MEMORY,
            102400, new MockStateMachine());
        DLedgerServer dLedgerServer3 = launchServerWithStateMachineDisableSnapshot(group, peers, "n2", selfId, DLedgerConfig.MEMORY,
            102400, new MockStateMachine());
        await().atMost(2, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> dLedgerServer1.getMemberState().isLeader());
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            WriteTask task = new WriteTask();
            task.setBody("hello".getBytes());
            final int index = i;
            dLedgerServer1.handleWrite(task, new MockWriteClosure() {
                @Override
                public void done(Status status) {
                    Assertions.assertTrue(status.isOk());
                    Assertions.assertEquals(index + 1, totalEntries);
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        }
    }

    @Test
    public void testSingleServerInFile() {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = "n0-localhost:" + nextPort();
        DLedgerServer dLedgerServer = launchServerWithStateMachineDisableSnapshot(group, peers, selfId, selfId, DLedgerConfig.FILE,
            102400, new MockStateMachine());
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            WriteTask task = new WriteTask();
            task.setBody("hello".getBytes());
            final int index = i;
            dLedgerServer.handleWrite(task, new MockWriteClosure() {
                @Override
                public void done(Status status) {
                    Assertions.assertTrue(status.isOk());
                    Assertions.assertEquals(index + 1, totalEntries);
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        }
    }

    @Test
    public void testThreeServerInFile() {
        String group = UUID.randomUUID().toString();
        String selfId = "n0";
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer1 = launchServerWithStateMachineDisableSnapshot(group, peers, "n0", selfId, DLedgerConfig.FILE,
            102400, new MockStateMachine());
        DLedgerServer dLedgerServer2 = launchServerWithStateMachineDisableSnapshot(group, peers, "n1", selfId, DLedgerConfig.FILE,
            102400, new MockStateMachine());
        DLedgerServer dLedgerServer3 = launchServerWithStateMachineDisableSnapshot(group, peers, "n2", selfId, DLedgerConfig.FILE,
            102400, new MockStateMachine());
        await().atMost(2, TimeUnit.SECONDS).pollInterval(300, TimeUnit.MILLISECONDS).until(() -> dLedgerServer1.getMemberState().isLeader());
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            WriteTask task = new WriteTask();
            task.setBody("hello".getBytes());
            final int index = i;
            dLedgerServer1.handleWrite(task, new MockWriteClosure() {
                @Override
                public void done(Status status) {
                    Assertions.assertTrue(status.isOk());
                    Assertions.assertEquals(index + 1, totalEntries);
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.assertTrue(false);
        }
    }

}
