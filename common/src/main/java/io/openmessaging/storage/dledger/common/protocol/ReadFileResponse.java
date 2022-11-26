/*
 * Copyright 2017-2022 The DLedger Authors
 *
 * Licensed under the Apache License; Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing; software
 * distributed under the License is distributed on an "AS IS" BASIS;
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND; either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.common.protocol;

import io.openmessaging.storage.dledger.common.entry.DLedgerEntry;

public class ReadFileResponse extends RequestOrResponse {

    private DLedgerEntry dLedgerEntry;

    @Override
    public ReadFileResponse code(int code) {
        this.code = code;
        return ReadFileResponse.this;
    }

    public DLedgerEntry getDLedgerEntry() {
        return dLedgerEntry;
    }

    public void setDLedgerEntry(DLedgerEntry dLedgerEntry) {
        this.dLedgerEntry = dLedgerEntry;
    }
}
