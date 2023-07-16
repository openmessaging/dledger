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
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.common.ShutdownAbleThread;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotStatus;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.snapshot.SnapshotMeta;
import io.openmessaging.storage.dledger.snapshot.hook.LoadSnapshotHook;
import io.openmessaging.storage.dledger.snapshot.hook.SaveSnapshotHook;
import io.openmessaging.storage.dledger.snapshot.hook.SnapshotHook;
import io.openmessaging.storage.dledger.store.DLedgerStore;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finite state machine caller Through a task queue, all tasks that modify the state of the state machine are guaranteed
 * to be executed sequentially.
 */
public class StateMachineCaller extends ShutdownAbleThread {

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
        SnapshotHook snapshotHook;
    }

    private static final long RETRY_ON_COMMITTED_DELAY = 1000;
    private static Logger logger = LoggerFactory.getLogger(StateMachineCaller.class);
    private final DLedgerStore dLedgerStore;
    private final StateMachine statemachine;
    private final DLedgerEntryPusher entryPusher;

    private final MemberState memberState;
    private final BlockingQueue<ApplyTask> taskQueue;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "RetryOnCommittedScheduledThread");
            }
        });
    private final Function<ApplyEntry, Boolean> completeEntryCallback;
    private volatile DLedgerException error;
    private Optional<SnapshotManager> snapshotManager;

    public StateMachineCaller(final DLedgerStore dLedgerStore, final StateMachine statemachine,
        final DLedgerEntryPusher entryPusher) {
        super("StateMachineCaller-" + dLedgerStore.getMemberState().getSelfId(), logger);
        this.dLedgerStore = dLedgerStore;
        this.statemachine = statemachine;
        this.entryPusher = entryPusher;
        this.memberState = dLedgerStore.getMemberState();
        this.taskQueue = new LinkedBlockingQueue<>(1024);
        if (entryPusher != null) {
            this.completeEntryCallback = entryPusher::completeResponseFuture;
            entryPusher.registerStateMachine(this);
        } else {
            this.completeEntryCallback = entry -> true;
        }
        this.snapshotManager = Optional.empty();
    }

    private boolean enqueueTask(final ApplyTask task) {
        return this.taskQueue.offer(task);
    }

    public StateMachine getStateMachine() {
        return this.statemachine;
    }

    public boolean onCommitted(final long committedIndex) {
        if (committedIndex <= this.memberState.getAppliedIndex())
            return false;
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.COMMITTED;
        task.committedIndex = committedIndex;
        return enqueueTask(task);
    }

    public boolean onSnapshotLoad(final LoadSnapshotHook loadSnapshotAfter) {
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.SNAPSHOT_LOAD;
        task.snapshotHook = loadSnapshotAfter;
        return enqueueTask(task);
    }

    public boolean onSnapshotSave(final SaveSnapshotHook saveSnapshotAfter) {
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.SNAPSHOT_SAVE;
        task.snapshotHook = saveSnapshotAfter;
        return enqueueTask(task);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.statemachine.onShutdown();
    }

    @Override
    public void doWork() {
        try {
            final ApplyTask task = this.taskQueue.poll(5, TimeUnit.SECONDS);
            if (task != null) {
                switch (task.type) {
                    case COMMITTED:
                        doCommitted(task.committedIndex);
                        break;
                    case SNAPSHOT_SAVE:
                        doSnapshotSave((SaveSnapshotHook) task.snapshotHook);
                        break;
                    case SNAPSHOT_LOAD:
                        doSnapshotLoad((LoadSnapshotHook) task.snapshotHook);
                        break;
                }
            }
        } catch (final InterruptedException e) {
            logger.error("Error happen in stateMachineCaller when pull task from task queue", e);
        } catch (Throwable e) {
            logger.error("Apply task exception", e);
        }
    }

    private void doCommitted(final long committedIndex) {
        if (this.error != null) {
            return;
        }
        final long lastAppliedIndex = this.memberState.getAppliedIndex();
        if (lastAppliedIndex >= committedIndex) {
            return;
        }
        if (this.snapshotManager.isPresent() && (this.snapshotManager.get().isLoadingSnapshot() || this.snapshotManager.get().isSavingSnapshot())) {
            this.scheduledExecutorService.schedule(() -> {
                try {
                    onCommitted(committedIndex);
                    logger.info("Still loading or saving snapshot, retry the commit task with index: {} later", committedIndex);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }, RETRY_ON_COMMITTED_DELAY, TimeUnit.MILLISECONDS);
            return;
        }
        final ApplyEntryIterator iter = new ApplyEntryIterator(this.dLedgerStore, committedIndex, lastAppliedIndex, this.completeEntryCallback);
        this.statemachine.onApply(iter);
        final long lastIndex = iter.getIndex();
        DLedgerEntry entry = this.dLedgerStore.get(lastIndex);
        this.memberState.updateAppliedIndexAndTerm(lastIndex, entry.getTerm());
        // Take snapshot
        snapshotManager.ifPresent(x -> x.saveSnapshot());
        // Check response timeout.
        if (iter.getCompleteAckNums() == 0) {
            if (this.entryPusher != null) {
                this.entryPusher.checkResponseFuturesTimeout(this.memberState.getAppliedIndex() + 1);
            }
        }
    }

    private void doSnapshotLoad(LoadSnapshotHook loadSnapshotAfter) {
        // Get snapshot meta
        SnapshotReader reader = loadSnapshotAfter.getSnapshotReader();
        SnapshotMeta snapshotMeta;
        try {
            snapshotMeta = reader.load();
        } catch (IOException e) {
            logger.error(e.getMessage());
            loadSnapshotAfter.doCallBack(SnapshotStatus.FAIL);
            return;
        }
        if (snapshotMeta == null) {
            logger.error("Unable to load state machine meta");
            loadSnapshotAfter.doCallBack(SnapshotStatus.FAIL);
            return;
        }
        // Compare snapshot meta with the last applied index and term
        long snapshotIndex = snapshotMeta.getLastIncludedIndex();
        long snapshotTerm = snapshotMeta.getLastIncludedTerm();
        if (snapshotIndex <= this.memberState.getAppliedIndex()) {
            logger.warn("The snapshot loading is expired");
            loadSnapshotAfter.doCallBack(SnapshotStatus.EXPIRED);
            return;
        }
        // Load data from the state machine
        try {
            if (!this.statemachine.onSnapshotLoad(reader)) {
                logger.error("Unable to load data from snapshot into state machine");
                loadSnapshotAfter.doCallBack(SnapshotStatus.FAIL);
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
            loadSnapshotAfter.doCallBack(SnapshotStatus.FAIL);
            return;
        }
        // Update statemachine info
        this.memberState.updateAppliedIndexAndTerm(snapshotIndex, snapshotTerm);
        this.memberState.leaderUpdateCommittedIndex(snapshotTerm, snapshotIndex);
        loadSnapshotAfter.registerSnapshotMeta(snapshotMeta);
        loadSnapshotAfter.doCallBack(SnapshotStatus.SUCCESS);
    }

    private void doSnapshotSave(SaveSnapshotHook saveSnapshotAfter) {
        // Build and save snapshot meta
        saveSnapshotAfter.registerSnapshotMeta(new SnapshotMeta(this.memberState.getAppliedIndex(), this.memberState.getAppliedTerm()));
        SnapshotWriter writer = saveSnapshotAfter.getSnapshotWriter();
        if (writer == null) {
            return;
        }
        // Save data through the state machine
        try {
            if (!this.statemachine.onSnapshotSave(writer)) {
                logger.error("Unable to save snapshot data from state machine");
                saveSnapshotAfter.doCallBack(SnapshotStatus.FAIL);
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
            saveSnapshotAfter.doCallBack(SnapshotStatus.FAIL);
            return;
        }
        saveSnapshotAfter.doCallBack(SnapshotStatus.SUCCESS);
    }

    public void setError(DLedgerServer server, final DLedgerException error) {
        this.error = error;
        if (this.statemachine != null) {
            this.statemachine.onError(error);
        }
        if (server != null) {
            server.shutdown();
        }
    }

    public void registerSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = Optional.of(snapshotManager);
    }

    public SnapshotManager getSnapshotManager() {
        return this.snapshotManager.orElse(null);
    }

    public DLedgerStore getdLedgerStore() {
        return dLedgerStore;
    }
}
