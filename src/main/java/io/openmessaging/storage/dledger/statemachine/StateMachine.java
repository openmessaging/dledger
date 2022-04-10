package io.openmessaging.storage.dledger.statemachine;

import java.util.concurrent.CompletableFuture;

import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;

/**
 * Finite state machine, which should be implemented by user.
 */
public interface StateMachine {

    /**
     * Update the user statemachine with a batch a tasks that can be accessed
     * through |iterator|.
     *
     * @param iter iterator of committed entry
     */
    void onApply(final CommittedEntryIterator iter);

    /**
     * User defined snapshot generate function, this method will block StateMachine#onApply(Iterator).
     * Call done.run(status) when snapshot finished.
     *
     * @param writer snapshot writer
     * @param done   callback
     */
    void onSnapshotSave(final SnapshotWriter writer, final CompletableFuture<Boolean> done);

    /**
     * User defined snapshot load function.
     *
     * @param reader snapshot reader
     * @return true on success
     */
    boolean onSnapshotLoad(final SnapshotReader reader);

    /**
     * Invoked once when the raft node was shut down.
     * Default do nothing
     */
    void onShutdown();
}
