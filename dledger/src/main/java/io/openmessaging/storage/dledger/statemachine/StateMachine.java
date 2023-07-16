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

import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;

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
    void onApply(final ApplyEntryIterator iter);

    /**
     * User defined snapshot generate function.
     *
     * @param writer snapshot writer
     */
    boolean onSnapshotSave(final SnapshotWriter writer);

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

    /**
     * Once a critical error occurs, disallow any task enter the Dledger node
     * until the error has been fixed and restart it.
     *
     * @param error DLedger error message
     */
    void onError(final DLedgerException error);

    /**
     * User must create DLedgerId by this method, it will generate the DLedgerId with format like that: 'dLedgerGroupId#dLedgerSelfId'
     * @param dLedgerGroupId the group id of the DLedgerServer
     * @param dLedgerSelfId the self id of the DLedgerServer
     * @return generated unique DLedgerId
     */
    default String generateDLedgerId(String dLedgerGroupId, String dLedgerSelfId) {
        return DLedgerUtils.generateDLedgerId(dLedgerGroupId, dLedgerSelfId);
    }

    /**
     * User should return the DLedgerId which can be created by the method 'StateMachine#generateDLedgerId'
     * @return DLedgerId
     */
    String getBindDLedgerId();
}
