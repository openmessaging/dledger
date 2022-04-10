package io.openmessaging.storage.dledger.statemachine;

/**
 * Finite state machine, which should be implemented by user.
 */
public interface StateMachine {


    /**
     * Update the user statemachine with a batch a tasks that can be accessed
     * through |iterator|.
     * @param iter iterator of committed entry
     */
    void onApply(final CommittedEntryIterator iter);
}
