/*
 * Copyright 2017-2022 The DLedger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.core.statemachine;

import io.openmessaging.storage.dledger.common.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.common.exception.DLedgerException;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.statemachine.CommittedEntryIterator;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockStateMachine implements StateMachine {

    private static Logger logger = LoggerFactory.getLogger(MockStateMachine.class);
    private volatile long appliedIndex = -1L;
    private final AtomicLong totalEntries = new AtomicLong(0);
    private final AtomicLong lastAppliedIndex = new AtomicLong(-1);

    @Override
    public void onApply(final CommittedEntryIterator iter) {
        while (iter.hasNext()) {
            final DLedgerEntry next = iter.next();
            if (next != null) {
                if (next.getIndex() <= this.appliedIndex) {
                    continue;
                }
                this.totalEntries.addAndGet(1);
                this.appliedIndex = next.getIndex();
            }
        }
    }

    @Override
    public boolean onSnapshotSave(final SnapshotWriter writer) {
        long curEntryCnt = this.totalEntries.get();
        MockSnapshotFile snapshotFile = new MockSnapshotFile(writer.getSnapshotStorePath() + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        return snapshotFile.save(curEntryCnt);
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        // Apply snapshot data
        MockSnapshotFile snapshotFile = new MockSnapshotFile(reader.getSnapshotStorePath() +
                File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        try {
            this.totalEntries.set(snapshotFile.load());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
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
}
