/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

public class NoOpStatemachine implements StateMachine {
    @Override
    public void onApply(ApplyEntryIterator iter) {
        while (iter.hasNext()) {
            iter.next();
        }
    }

    @Override
    public boolean onSnapshotSave(SnapshotWriter writer) {
        return false;
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        return false;
    }

    @Override
    public void onShutdown() {

    }

    @Override
    public void onError(DLedgerException error) {

    }

    @Override
    public String getBindDLedgerId() {
        return null;
    }
}
