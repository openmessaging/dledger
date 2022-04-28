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

package io.openmessaging.storage.dledger.statemachine;

import io.openmessaging.storage.dledger.DLedgerEntryPusher;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finite state machine caller
 * Through a task queue, all tasks that modify the state of the state machine
 * are guaranteed to be executed sequentially.
 */
public class StateMachineCaller extends ServiceThread {

    /**
     * Task type
     */
    private enum TaskType {
        COMMITTED,
        SNAPSHOT_SAVE,
        SNAPSHOT_LOAD,
        SHUTDOWN,
    }

    /**
     * Apply task, which updates state machine's state
     */
    private static class ApplyTask {
        TaskType type;
        long committedIndex;
        long term;
        CompletableFuture<Boolean> cb;
    }

    private static Logger logger = LoggerFactory.getLogger(StateMachineCaller.class);
    private final DLedgerStore dLedgerStore;
    private final StateMachine statemachine;
    private final DLedgerEntryPusher entryPusher;
    private final AtomicLong lastAppliedIndex;
    private long lastAppliedTerm;
    private final AtomicLong applyingIndex;
    private final BlockingQueue<ApplyTask> taskQueue;
    private final Function<Long, Boolean> completeEntryCallback;

    public StateMachineCaller(final DLedgerStore dLedgerStore, final StateMachine statemachine,
        final DLedgerEntryPusher entryPusher) {
        this.dLedgerStore = dLedgerStore;
        this.statemachine = statemachine;
        this.entryPusher = entryPusher;
        this.lastAppliedIndex = new AtomicLong(-1);
        this.applyingIndex = new AtomicLong(-1);
        this.taskQueue = new LinkedBlockingQueue<>(1024);
        if (entryPusher != null) {
            this.completeEntryCallback = entryPusher::completeResponseFuture;
        } else {
            this.completeEntryCallback = (index) -> true;
        }
    }

    private boolean enqueueTask(final ApplyTask task) {
        return this.taskQueue.offer(task);
    }

    public StateMachine getStateMachine() {
        return this.statemachine;
    }

    public boolean onCommitted(final long committedIndex) {
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.COMMITTED;
        task.committedIndex = committedIndex;
        return enqueueTask(task);
    }

    public boolean onSnapshotLoad(final CompletableFuture<Boolean> cb) {
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.SNAPSHOT_LOAD;
        task.cb = cb;
        return enqueueTask(task);
    }

    public boolean onSnapshotSave(final CompletableFuture<Boolean> cb) {
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.SNAPSHOT_SAVE;
        task.cb = cb;
        return enqueueTask(task);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.statemachine.onShutdown();
    }

    @Override
    public void run() {
        while (!this.isStopped()) {
            try {
                final ApplyTask task = this.taskQueue.poll(5, TimeUnit.SECONDS);
                if (task != null) {
                    switch (task.type) {
                        case COMMITTED:
                            doCommitted(task.committedIndex);
                            break;
                        case SNAPSHOT_SAVE:
                            doSnapshotSave(task.cb);
                            break;
                        case SNAPSHOT_LOAD:
                            doSnapshotLoad(task.cb);
                            break;
                    }
                }
            } catch (final InterruptedException e) {
                logger.error("Error happen in {} when pull task from task queue", getServiceName(), e);
            }
        }
    }

    private void doCommitted(final long committedIndex) {
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        if (lastAppliedIndex >= committedIndex) {
            return;
        }
        final CommittedEntryIterator iter = new CommittedEntryIterator(this.dLedgerStore, committedIndex, this.applyingIndex, lastAppliedIndex, this.completeEntryCallback);
        while (iter.hasNext()) {
            this.statemachine.onApply(iter);
        }
        final long lastIndex = iter.getIndex();
        this.lastAppliedIndex.set(lastIndex);
        final DLedgerEntry dLedgerEntry = this.dLedgerStore.get(lastIndex);
        if (dLedgerEntry != null) {
            this.lastAppliedTerm = dLedgerEntry.getTerm();
        }

        // Check response timeout.
        if (iter.getCompleteAckNums() == 0) {
            if (this.entryPusher != null) {
                this.entryPusher.checkResponseFuturesTimeout(this.lastAppliedIndex.get() + 1);
            }
        }
    }

    private void doSnapshotLoad(final CompletableFuture<Boolean> cb) {
    }

    private void doSnapshotSave(final CompletableFuture<Boolean> cb) {
    }

    @Override
    public String getServiceName() {
        return StateMachineCaller.class.getName();
    }

    public Long getLastAppliedIndex() {
        return this.lastAppliedIndex.get();
    }
}
