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

package io.openmessaging.storage.dledger.entry;

/**
 * DLedgerEntryType, upper layer can only see the NORMAL entry, the other two are used internally.
 */
public enum DLedgerEntryType {

    /**
     * The entry which contains the upper layer business data.
     */
    NORMAL(1),

    /**
     * The entry with empty body, used for RaftLog-Read and commit index fast advanced when the leader is changed.
     */
    NOOP(2),

    /**
     * TODO: The entry used for configuration change.
     */
    CONFIG_CHANGE(3);

    int magic;

    DLedgerEntryType(int magic) {
        this.magic = magic;
    }

    public int getMagic() {
        return magic;
    }

    public static boolean isValid(int magic) {
        return magic == NORMAL.getMagic() || magic == NOOP.getMagic() || magic == CONFIG_CHANGE.getMagic();
    }
}
