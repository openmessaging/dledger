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

import java.util.concurrent.CompletableFuture;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;

public class MockStateMachine implements StateMachine {

    private volatile long appliedIndex = -1;
    private volatile long totalEntries;

    @Override
    public void onApply(final CommittedEntryIterator iter) {
        while (iter.hasNext()) {
            final DLedgerEntry next = iter.next();
            if (next != null) {
                if (next.getIndex() <= this.appliedIndex) {
                    continue;
                }
                this.appliedIndex = next.getIndex();
                this.totalEntries += 1;
            }
        }
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final CompletableFuture<Boolean> done) {
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        return false;
    }

    @Override
    public void onShutdown() {

    }

    public long getAppliedIndex() {
        return this.appliedIndex;
    }

    public long getTotalEntries() {
        return totalEntries;
    }
}
