/*
 * Copyright 2017-2020 the original author or authors.
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

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.snapshot.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class MockStateMachine implements StateMachine {

    private static final Logger logger = LoggerFactory.getLogger(MockStateMachine.class);
    private volatile long appliedIndex = -1L;

    private volatile long lastSnapshotIncludedIndex = -1L;
    private final AtomicLong totalEntries = new AtomicLong(0);

    @Override
    public void onApply(final ApplyEntryIterator iter) {
        while (iter.hasNext()) {
            final ApplyEntry applyEntry = iter.next();
            if (applyEntry != null) {
                final DLedgerEntry dLedgerEntry = applyEntry.getEntry();
                if (dLedgerEntry.getIndex() <= this.appliedIndex) {
                    continue;
                }
                long nowEntries = this.totalEntries.addAndGet(1);
                this.appliedIndex = dLedgerEntry.getIndex();
                applyEntry.setResp(nowEntries);
                logger.info("apply index: {}",this.appliedIndex);
                logger.info("total entries: {}", nowEntries);
            }
        }
    }

    @Override
    public boolean onSnapshotSave(final SnapshotWriter writer) {
        long curEntryCnt = this.totalEntries.get();
        this.lastSnapshotIncludedIndex = this.appliedIndex;
        MockSnapshotFile snapshotFile = new MockSnapshotFile(writer.getSnapshotStorePath() + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        logger.info("save snapshot, lastIncludedIndex: {}, total entries: {}", this.lastSnapshotIncludedIndex, curEntryCnt);
        return snapshotFile.save(curEntryCnt);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        // Apply snapshot data
        MockSnapshotFile snapshotFile = new MockSnapshotFile(reader.getSnapshotStorePath() +
                File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        try {
            this.totalEntries.set(snapshotFile.load());
            this.appliedIndex = reader.getSnapshotMeta().getLastIncludedIndex();
            this.lastSnapshotIncludedIndex = this.appliedIndex;
            logger.info("load snapshot, lastIncludedIndex: {}, total entries: {}", this.appliedIndex, this.totalEntries.get());
            return true;
        } catch (IOException e) {
            logger.error("load snapshot failed", e);
            return false;
        }
    }

    @Override
    public void onShutdown() {

    }

    @Override
    public void onError(DLedgerException error) {
        logger.error("DLedger Error: {}", error.getMessage(), error);
    }

    @Override
    public String getBindDLedgerId() {
        return null;
    }

    public long getAppliedIndex() {
        return this.appliedIndex;
    }

    public long getTotalEntries() {
        return this.totalEntries.get();
    }

    public long getLastSnapshotIncludedIndex() {
        return lastSnapshotIncludedIndex;
    }
}
