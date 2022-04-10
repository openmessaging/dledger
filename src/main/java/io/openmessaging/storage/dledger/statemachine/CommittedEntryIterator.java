package io.openmessaging.storage.dledger.statemachine;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.store.DLedgerStore;

/**
 * The iterator implementation of committed entries.
 */
public class CommittedEntryIterator implements Iterator<DLedgerEntry> {

    private final DLedgerStore dLedgerStore;
    private final long         committedIndex;
    private final AtomicLong   applyingIndex;
    private long               currentIndex;

    public CommittedEntryIterator(final DLedgerStore dLedgerStore, final long committedIndex,
                                  final AtomicLong applyingIndex, final long lastAppliedIndex) {
        this.dLedgerStore = dLedgerStore;
        this.committedIndex = committedIndex;
        this.applyingIndex = applyingIndex;
        this.currentIndex = lastAppliedIndex;
    }

    @Override
    public boolean hasNext() {
        return this.currentIndex < this.committedIndex;
    }

    @Override
    public DLedgerEntry next() {
        ++this.currentIndex;
        if (this.currentIndex <= this.committedIndex) {
            final DLedgerEntry dLedgerEntry = this.dLedgerStore.get(this.currentIndex);
            this.applyingIndex.set(this.currentIndex);
            return dLedgerEntry;
        }
        return null;
    }

    public long getIndex() {
        return this.currentIndex;
    }
}
