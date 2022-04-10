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
