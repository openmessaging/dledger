package io.openmessaging.storage.dledger.statemachine;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.remoting.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.store.DLedgerStore;

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
    private final AtomicLong lastAppliedIndex;
    private long lastAppliedTerm;
    private final AtomicLong applyingIndex;
    private final BlockingQueue<ApplyTask> taskQueue;

    public StateMachineCaller(final DLedgerStore dLedgerStore, final StateMachine statemachine) {
        this.dLedgerStore = dLedgerStore;
        this.statemachine = statemachine;
        this.lastAppliedIndex = new AtomicLong(-1);
        this.applyingIndex = new AtomicLong(-1);
        this.taskQueue = new LinkedBlockingQueue<>(1024);
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
        final CommittedEntryIterator iter = new CommittedEntryIterator(this.dLedgerStore, committedIndex, this.applyingIndex, lastAppliedIndex);
        while (iter.hasNext()) {
            this.statemachine.onApply(iter);
        }
        final long lastIndex = iter.getIndex();
        this.lastAppliedIndex.set(lastIndex);
        final DLedgerEntry dLedgerEntry = this.dLedgerStore.get(lastIndex);
        if (dLedgerEntry != null) {
            this.lastAppliedTerm = dLedgerEntry.getTerm();
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
}
