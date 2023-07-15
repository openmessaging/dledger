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

package io.openmessaging.storage.dledger.snapshot;

import io.openmessaging.storage.dledger.DLedgerConfig;

public enum SnapshotEntryResetStrategy {
    /**
     * Delete all entries in (..., lastIncludedIndex] in sync mode
     */
    RESET_ALL_SYNC,
    /**
     * Delete all entries in (..., lastIncludedIndex] in async mode
     */
    RESET_ALL_ASYNC,

    /**
     * Delete all entries in (..., lastIncludedIndex] in the configured time {@link DLedgerConfig#getResetSnapshotEntriesDelayTime()}
     */
    RESET_ALL_LATER,

    /**
     * Delete all entries in (..., lastIncludedIndex - keepEntriesNum {@link DLedgerConfig#getResetSnapshotEntriesButKeepLastEntriesNum()}] in sync mode
     */
    RESET_BUT_KEEP_SOME_SYNC,

    /**
     * Delete all entries in (..., lastIncludedIndex - keepEntriesNum {@link DLedgerConfig#getResetSnapshotEntriesButKeepLastEntriesNum()}] in async mode
     */
    RESET_BUT_KEEP_SOME_ASYNC,

    /**
     * Delete all entries in (..., lastIncludedIndex - keepEntriesNum {@link DLedgerConfig#getResetSnapshotEntriesButKeepLastEntriesNum()}] in the configured time {@link DLedgerConfig#getResetSnapshotEntriesDelayTime()}
     */
    RESET_BUT_KEEP_SOME_LATER,
}