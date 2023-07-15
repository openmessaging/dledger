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

package io.openmessaging.storage.dledger.protocol;

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.utils.PreConditions;
import java.util.ArrayList;
import java.util.List;

public class PushEntryRequest extends RequestOrResponse {
    private long preLogIndex = -1;
    private long preLogTerm = -1;
    private long commitIndex = -1;
    private Type type = Type.APPEND;

    private List<DLedgerEntry> entries = new ArrayList<>();
    private int totalSize;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getPreLogIndex() {
        return preLogIndex;
    }

    public void setPreLogIndex(long preLogIndex) {
        this.preLogIndex = preLogIndex;
    }

    public long getPreLogTerm() {
        return preLogTerm;
    }

    public void setPreLogTerm(long preLogTerm) {
        this.preLogTerm = preLogTerm;
    }

    public void addEntry(DLedgerEntry entry) {
        if (!entries.isEmpty()) {
            PreConditions.check(entries.get(entries.size() - 1).getIndex() + 1 == entry.getIndex(), DLedgerResponseCode.UNKNOWN, "batch push in wrong order");
        }
        entries.add(entry);
        totalSize += entry.getSize();
    }

    public long getFirstEntryIndex() {
        if (!entries.isEmpty()) {
            return entries.get(0).getIndex();
        } else {
            return -1;
        }
    }

    public long getLastEntryIndex() {
        if (!entries.isEmpty()) {
            return entries.get(entries.size() - 1).getIndex();
        } else {
            return -1;
        }
    }

    public long getLastEntryTerm() {
        if (!entries.isEmpty()) {
            return entries.get(entries.size() - 1).getTerm();
        } else {
            return -1;
        }
    }

    public int getCount() {
        return entries.size();
    }

    public long getTotalSize() {
        return totalSize;
    }

    public List<DLedgerEntry> getEntries() {
        return entries;
    }

    public void clear() {
        entries.clear();
        totalSize = 0;
    }

    public enum Type {
        APPEND,
        COMMIT,
        COMPARE,
        TRUNCATE,
        INSTALL_SNAPSHOT
    }
}
