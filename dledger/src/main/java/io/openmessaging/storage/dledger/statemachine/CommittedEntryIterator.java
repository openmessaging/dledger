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

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.entry.DLedgerEntryType;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * The iterator implementation of committed entries.
 */
public class CommittedEntryIterator implements Iterator<DLedgerEntry> {
    private final CommittedEntryIteratorInner inner;
    private DLedgerEntry nextEntry;

    private final Predicate<DLedgerEntry> filter = new Predicate<DLedgerEntry>() {
        @Override
        public boolean test(DLedgerEntry entry) {
            // only normal entry can deliver to upper statemachine for applying
            return entry.getMagic() == DLedgerEntryType.NORMAL.getMagic();
        }
    };

    public CommittedEntryIterator(final DLedgerStore dLedgerStore, final long committedIndex, final long lastAppliedIndex,
        final Function<Long, Boolean> completeEntryCallback) {
        this.inner = new CommittedEntryIteratorInner(dLedgerStore, committedIndex, lastAppliedIndex, completeEntryCallback);
    }

    public long getIndex() {
        return inner.getIndex();
    }

    public int getCompleteAckNums() {
        return inner.getCompleteAckNums();
    }

    @Override
    public boolean hasNext() {
        while (inner.hasNext()) {
            DLedgerEntry dLedgerEntry = inner.next();
            if (filter.test(dLedgerEntry)) {
                nextEntry = dLedgerEntry;
                return true;
            }
        }
        return false;
    }

    @Override
    public DLedgerEntry next() {
        DLedgerEntry entry = nextEntry;
        nextEntry = null;
        return entry;
    }

    private static class CommittedEntryIteratorInner implements Iterator<DLedgerEntry> {

        private final Function<Long, Boolean> completeEntryCallback;
        private final DLedgerStore dLedgerStore;
        private final long committedIndex;
        private final long firstApplyingIndex;
        private long currentIndex;
        private int completeAckNums = 0;

        private CommittedEntryIteratorInner(final DLedgerStore dLedgerStore, final long committedIndex, final long lastAppliedIndex,
            final Function<Long, Boolean> completeEntryCallback) {
            this.dLedgerStore = dLedgerStore;
            this.committedIndex = committedIndex;
            this.firstApplyingIndex = lastAppliedIndex + 1;
            this.currentIndex = lastAppliedIndex;
            this.completeEntryCallback = completeEntryCallback;
        }

        @Override
        public boolean hasNext() {
            if (this.currentIndex >= this.firstApplyingIndex && this.currentIndex <= this.committedIndex) {
                completeApplyingEntry();
            }
            if (this.currentIndex >= this.committedIndex) {
                return false;
            }
            return this.currentIndex < this.committedIndex;
        }

        @Override
        public DLedgerEntry next() {
            ++this.currentIndex;
            if (this.currentIndex <= this.committedIndex) {
                final DLedgerEntry dLedgerEntry = this.dLedgerStore.get(this.currentIndex);
                return dLedgerEntry;
            }
            return null;
        }

        private void completeApplyingEntry() {
            if (this.completeEntryCallback.apply(this.currentIndex)) {
                this.completeAckNums++;
            }
        }

        public long getIndex() {
            return this.currentIndex;
        }

        public int getCompleteAckNums() {
            return completeAckNums;
        }

    }
}
