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

import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import java.util.concurrent.CompletableFuture;

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
