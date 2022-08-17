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
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriterImpl;
import io.openmessaging.storage.dledger.snapshot.SnapshotReaderImpl;

import com.alibaba.fastjson.JSON;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

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
    private long lastSnapshotIndex;
    private long snapshotThreshold;
    private CommittedEntryIterator iter_;
    private long loadSnapshotTimes = 0;

    public StateMachineCaller(final DLedgerStore dLedgerStore, final StateMachine statemachine,
        final DLedgerEntryPusher entryPusher) {
        this.dLedgerStore = dLedgerStore;
        this.statemachine = statemachine;
        this.entryPusher = entryPusher;
        this.lastAppliedIndex = new AtomicLong(-1);
        this.lastSnapshotIndex = -1;
        this.snapshotThreshold = 100;
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

    public boolean onSnapshotLoad() {
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.SNAPSHOT_LOAD;
        return enqueueTask(task);
    }

    public boolean onSnapshotSave() {
        final ApplyTask task = new ApplyTask();
        task.type = TaskType.SNAPSHOT_SAVE;
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
                            doSnapshotSave();
                            break;
                        case SNAPSHOT_LOAD:
                            doSnapshotLoad();
                            break;
                    }
                }
            } catch (final InterruptedException e) {
                logger.error("Error happen in {} when pull task from task queue", getServiceName(), e);
            } catch (Throwable e) {
                logger.error("Apply task exception", e);
            }
        }
    }

    private void doSnapshottrigger(){
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        final long lastSnapshotIndex = this.lastSnapshotIndex;

        if( this.snapshotThreshold >= lastAppliedIndex - lastSnapshotIndex){
            return;
        } else{
            onSnapshotSave();
            this.lastSnapshotIndex = lastAppliedIndex;
        }
    }

    public Boolean cleanLogs(){
        // todo
        // long index = lastSnapshotIndex;
        // while (this.iter_.hasNext()) {
        //     final DLedgerEntry next = iter_.next();
        //     if (next != null) {
        //            dLedgerStore.truncate(entry, leaderTerm, leaderId);
        //     }
        // }

        if(this.dLedgerStore instanceof DLedgerMmapFileStore){
            DLedgerMmapFileStore dLedgerMmapFileStore = (DLedgerMmapFileStore)this.dLedgerStore;
            dLedgerMmapFileStore.reset(lastSnapshotIndex * dLedgerMmapFileStore.INDEX_UNIT_SIZE);
        } else if(this.dLedgerStore instanceof DLedgerMemoryStore){
            ;
        }


        return true;
    }

    private void doCommitted(final long committedIndex) {
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        if (lastAppliedIndex >= committedIndex) {
            return;
        }
        final CommittedEntryIterator iter = new CommittedEntryIterator(this.dLedgerStore, committedIndex, this.applyingIndex, lastAppliedIndex, this.completeEntryCallback);
        this.iter_ = iter;
        while (iter.hasNext()) {
            this.statemachine.onApply(iter);
        }
        final long lastIndex = iter.getIndex();
        this.lastAppliedIndex.set(lastIndex);
        final DLedgerEntry dLedgerEntry = this.dLedgerStore.get(lastIndex);
        if (dLedgerEntry != null) {
            this.lastAppliedTerm = dLedgerEntry.getTerm();
        }

        // a snapshot trigger
        doSnapshottrigger();

        // Check response timeout.
        if (iter.getCompleteAckNums() == 0) {
            if (this.entryPusher != null) {
                this.entryPusher.checkResponseFuturesTimeout(this.lastAppliedIndex.get() + 1);
            }
        }
    }

    private void doSnapshotLoad() {
        SnapshotReaderImpl reader = new SnapshotReaderImpl();
        final long lastIncludedIndex_tmp = reader.getLastIncludedIndex();
        this.lastAppliedIndex.set(lastIncludedIndex_tmp);
        this.loadSnapshotTimes++;
        final String path = "./snapshot.data";
        final File file = new File(path);
        try (FileInputStream fin = new FileInputStream(file); BufferedInputStream in = new BufferedInputStream(fin);
            InputStreamReader inputstreamreader = new InputStreamReader(fin, "UTF-8");
            BufferedReader bufferedreader = new BufferedReader(inputstreamreader)) {
            String laststr = "";
            try {
                String tempString = null;
                while((tempString = bufferedreader.readLine()) != null){
                    laststr += tempString;
                }
                bufferedreader.close();
            } finally {
                ;
            }
            // read data
            List<DLedgerEntry> list = JSON.parseArray(laststr, DLedgerEntry.class);
            for(DLedgerEntry i:list){
                dLedgerStore.appendAsLeader(i);
            }
        } catch (final IOException e) {
            e.printStackTrace();
        }

    }

    private void doSnapshotSave() {
        long lastIncludedIndex = this.lastAppliedIndex.get();
        long lastIncludedTerm = this.lastAppliedTerm;
        final SnapshotWriterImpl writer = new SnapshotWriterImpl(lastIncludedIndex, lastIncludedTerm, this.iter_);
        writer.writeMeta();
        writer.writeData();
        // run clean log 
        cleanLogs();
    }

    @Override
    public String getServiceName() {
        return StateMachineCaller.class.getName();
    }

    public Long getLastAppliedIndex() {
        return this.lastAppliedIndex.get();
    }
}
