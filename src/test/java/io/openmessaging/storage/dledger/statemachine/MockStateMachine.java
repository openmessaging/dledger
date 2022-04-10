package io.openmessaging.storage.dledger.statemachine;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;

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
                totalEntries += 1;
            }
        }
    }

    public long getAppliedIndex() {
        return this.appliedIndex;
    }

    public long getTotalEntries() {
        return totalEntries;
    }
}
