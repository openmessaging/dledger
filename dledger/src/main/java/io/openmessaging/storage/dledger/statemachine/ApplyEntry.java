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

import io.openmessaging.storage.dledger.entry.DLedgerEntry;

public class ApplyEntry<T> {

    private DLedgerEntry entry;

    private T resp;

    public ApplyEntry(DLedgerEntry entry) {
        this.entry = entry;
    }

    public void setResp(T resp) {
        this.resp = resp;
    }

    public T getResp() {
        return resp;
    }

    public void setEntry(DLedgerEntry entry) {
        this.entry = entry;
    }

    public DLedgerEntry getEntry() {
        return entry;
    }
}
