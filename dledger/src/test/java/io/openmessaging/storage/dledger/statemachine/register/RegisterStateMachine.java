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

package io.openmessaging.storage.dledger.statemachine.register;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.snapshot.SnapshotReader;
import io.openmessaging.storage.dledger.snapshot.SnapshotWriter;
import io.openmessaging.storage.dledger.statemachine.ApplyEntry;
import io.openmessaging.storage.dledger.statemachine.ApplyEntryIterator;
import io.openmessaging.storage.dledger.statemachine.RegisterSnapshotFile;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import io.openmessaging.storage.dledger.utils.BytesUtil;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RegisterStateMachine implements StateMachine {

    // multi-register model, record the key-value pair
    private ConcurrentHashMap<Integer, Integer> register = new ConcurrentHashMap<>();

    @Override
    public void onApply(ApplyEntryIterator iter) {
        while (iter.hasNext()) {
            final ApplyEntry entry = iter.next();
            if (entry != null) {
                final DLedgerEntry dLedgerEntry = entry.getEntry();
                byte[] bytes = dLedgerEntry.getBody();
                int key = BytesUtil.bytesToInt(bytes, 0);
                int value = BytesUtil.bytesToInt(bytes, 4);
                register.put(key, value);
            }
        }
    }

    @Override
    public boolean onSnapshotSave(SnapshotWriter writer) {
        RegisterSnapshotFile registerSnapshotFile = new RegisterSnapshotFile(writer.getSnapshotStorePath() + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        return registerSnapshotFile.save(this.register);
    }

    @Override
    public boolean onSnapshotLoad(SnapshotReader reader) {
        RegisterSnapshotFile registerSnapshotFile = new RegisterSnapshotFile(reader.getSnapshotStorePath() + File.separator + SnapshotManager.SNAPSHOT_DATA_FILE);
        try{
            Map<Integer, Integer> register = registerSnapshotFile.load();
            this.register = new ConcurrentHashMap<>(register);
        } catch (Exception e) {
            return false;
        }
        return true;
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

    public Integer getValue(Integer key) {
        return this.register.getOrDefault(key, -1);
    }

    public ConcurrentHashMap<Integer, Integer> getRegister() {
        return register;
    }
}
