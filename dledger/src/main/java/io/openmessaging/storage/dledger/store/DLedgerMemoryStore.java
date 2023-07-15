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

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.utils.PreConditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerMemoryStore extends DLedgerStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerMemoryStore.class);

    private long ledgerBeforeBeginIndex = -1;
    private long ledgerEndIndex = -1;
    private long ledgerEndTerm;
    private final Map<Long, DLedgerEntry> cachedEntries = new ConcurrentHashMap<>();

    private final DLedgerConfig dLedgerConfig;
    private final MemberState memberState;

    public DLedgerMemoryStore(DLedgerConfig dLedgerConfig, MemberState memberState) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
    }

    @Override
    public MemberState getMemberState() {
        return this.memberState;
    }

    @Override
    public DLedgerEntry appendAsLeader(DLedgerEntry entry) {
        PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
        synchronized (memberState) {
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            PreConditions.check(memberState.getTransferee() == null, DLedgerResponseCode.LEADER_TRANSFERRING);
            ledgerEndIndex++;
            ledgerEndTerm = memberState.currTerm();
            entry.setIndex(ledgerEndIndex);
            entry.setTerm(memberState.currTerm());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Append as Leader {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            cachedEntries.put(entry.getIndex(), entry);
            updateLedgerEndIndexAndTerm();
            return entry;
        }
    }

    @Override
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return appendAsFollower(entry, leaderTerm, leaderId).getIndex();
    }

    @Override
    public long truncate(long truncateIndex) {
        for (long i = truncateIndex; i <= ledgerEndIndex ; i++) {
            this.cachedEntries.remove(truncateIndex);
        }
        DLedgerEntry entry = this.cachedEntries.get(truncateIndex - 1);
        if (entry == null) {
            ledgerEndIndex = -1;
            ledgerEndTerm = -1;
        } else {
            ledgerEndIndex = entry.getIndex();
            ledgerEndTerm = entry.getTerm();
        }
        return ledgerEndIndex;
    }

    @Override
    public long reset(long beforeBeginIndex, long beforeBeginTerm) {
        return 0;
    }

    @Override
    public void resetOffsetAfterSnapshot(DLedgerEntry entry) {

    }

    @Override
    public void updateIndexAfterLoadingSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        this.ledgerBeforeBeginIndex = lastIncludedIndex;
        this.ledgerEndIndex = lastIncludedIndex;
        this.ledgerEndTerm = lastIncludedTerm;
    }

    @Override
    public DLedgerEntry getFirstLogOfTargetTerm(long targetTerm, long endIndex) {
        DLedgerEntry entry = null;
        for (long i = endIndex; i > ledgerBeforeBeginIndex ; i--) {
            DLedgerEntry currentEntry = get(i);
            if (currentEntry == null) {
                continue;
            }
            if (currentEntry.getTerm() == targetTerm) {
                entry = currentEntry;
                continue;
            }
            if (currentEntry.getTerm() < targetTerm) {
                break;
            }
        }
        return entry;
    }

    @Override
    public void startup() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId) {
        PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
        synchronized (memberState) {
            PreConditions.check(memberState.isFollower(), DLedgerResponseCode.NOT_FOLLOWER);
            PreConditions.check(leaderTerm == memberState.currTerm(), DLedgerResponseCode.INCONSISTENT_TERM);
            PreConditions.check(leaderId.equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("[{}] Append as Follower {} {}", memberState.getSelfId(), entry.getIndex(), entry.getBody().length);
            }
            ledgerEndTerm = entry.getTerm();
            ledgerEndIndex = entry.getIndex();
            cachedEntries.put(entry.getIndex(), entry);
            updateLedgerEndIndexAndTerm();
            return entry;
        }

    }

    @Override
    public DLedgerEntry get(Long index) {
        return cachedEntries.get(index);
    }

    @Override
    public long getLedgerEndIndex() {
        return ledgerEndIndex;
    }

    @Override
    public long getLedgerBeforeBeginIndex() {
        return ledgerBeforeBeginIndex;
    }

    @Override
    public long getLedgerBeforeBeginTerm() {
        return 0;
    }

    @Override
    public void flush() {

    }

    @Override
    public long getLedgerEndTerm() {
        return ledgerEndTerm;
    }
}
