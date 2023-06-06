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

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.common.Closure;
import io.openmessaging.storage.dledger.common.ShutdownAbleThread;
import io.openmessaging.storage.dledger.common.Status;
import io.openmessaging.storage.dledger.common.TimeoutFuture;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerEntryPusher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    private final DLedgerConfig dLedgerConfig;
    private final DLedgerStore dLedgerStore;

    private final MemberState memberState;

    private final DLedgerRpcService dLedgerRpcService;

    private final Map<Long/*term*/, ConcurrentMap<String/*peer id*/, Long/*match index*/>> peerWaterMarksByTerm = new ConcurrentHashMap<>();

    private final Map<Long/*term*/, ConcurrentMap<Long/*index*/, Closure/*upper callback*/>> pendingClosure = new ConcurrentHashMap<>();

    private final EntryHandler entryHandler;

    private final QuorumAckChecker quorumAckChecker;

    private final Map<String/*peer id*/, EntryDispatcher/*entry dispatcher for each peer*/> dispatcherMap = new HashMap<>();

    private StateMachineCaller fsmCaller;

    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
        DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, LOGGER));
            }
        }
        this.entryHandler = new EntryHandler(LOGGER);
        this.quorumAckChecker = new QuorumAckChecker(LOGGER);
    }

    public void startup() {
        entryHandler.start();
        quorumAckChecker.start();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public void registerStateMachine(final StateMachineCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    private void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            LOGGER.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingClosure.containsKey(term)) {
            LOGGER.info("Initialize the pending closure map in {} for term={}", env, term);
            pendingClosure.putIfAbsent(term, new ConcurrentHashMap<>());
        }

    }

    private void updatePeerWaterMark(long term, String peerId, long index) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "updatePeerWaterMark");
            if (peerWaterMarksByTerm.get(term).get(peerId) < index) {
                peerWaterMarksByTerm.get(term).put(peerId, index);
            }
        }
    }

    public long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    public boolean isPendingFull(long currTerm) {
        checkTermForPendingMap(currTerm, "isPendingFull");
        return pendingClosure.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    public void appendClosure(Closure closure, long term, long index) {
        updatePeerWaterMark(term, memberState.getSelfId(), index);
        checkTermForPendingMap(term, "waitAck");
        Closure old = this.pendingClosure.get(term).put(index, closure);
        if (old != null) {
            LOGGER.warn("[MONITOR] get old wait at term = {}, index= {}", term, index);
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     * Complete the TimeoutFuture in pendingAppendResponsesByTerm (CurrentTerm, index).
     * Called by statemachineCaller when a committed entry (CurrentTerm, index) was applying to statemachine done.
     *
     * @return true if complete success
     */
    public boolean completeResponseFuture(final long index) {
        final long term = this.memberState.currTerm();
        ConcurrentMap<Long, Closure> closureMap = this.pendingClosure.get(term);
        if (closureMap != null) {
            Closure closure = closureMap.remove(index);
            if (closure != null) {
                closure.done(Status.ok());
                LOGGER.info("Complete closure, term = {}, index = {}", term, index);
                return true;
            }
        }
        return false;
    }

    /**
     * Check responseFutures timeout from {beginIndex} in currentTerm
     */
    public void checkResponseFuturesTimeout(final long beginIndex) {
        final long term = this.memberState.currTerm();
        ConcurrentMap<Long, Closure> closureMap = this.pendingClosure.get(term);
        if (closureMap != null) {
            for (long i = beginIndex; i < Integer.MAX_VALUE; i++) {
                Closure closure = closureMap.get(i);
                if (closure == null) {
                    break;
                } else if (closure.isTimeOut()) {
                    closure.done(Status.error(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT));
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Check responseFutures elapsed before {endIndex} in currentTerm
     */
    private void checkResponseFuturesElapsed(final long endIndex) {
        final long currTerm = this.memberState.currTerm();
        final Map<Long, Closure> closureMap = this.pendingClosure.get(currTerm);
        for (Map.Entry<Long, Closure> closureEntry : closureMap.entrySet()) {
            if (closureEntry.getKey() <= endIndex) {
                closureEntry.getValue().done(Status.ok());
                closureMap.remove(closureEntry.getKey());
            }
        }
    }

    /**
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker-" + memberState.getSelfId(), logger);
        }

        @Override
        public void doWork() {
            try {
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    logger.info("[{}][{}] term={} ledgerBeforeBegin={} ledgerEnd={} committed={} watermarks={} appliedIndex={}",
                        memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeforeBeginIndex(), dLedgerStore.getLedgerEndIndex(), memberState.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm), memberState.getAppliedIndex());
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                // clear pending closure in old term
                if (pendingClosure.size() > 1) {
                    for (Long term : pendingClosure.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        for (Map.Entry<Long, Closure> futureEntry : pendingClosure.get(term).entrySet()) {
                            logger.info("[TermChange] Will clear the pending closure index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().done(Status.error(DLedgerResponseCode.EXPIRED_TERM));
                        }
                        pendingClosure.remove(term);
                    }
                }
                // clear peer watermarks in old term
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        peerWaterMarksByTerm.remove(term);
                    }
                }

                // clear the pending closure which index <= applyIndex
                if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                    checkResponseFuturesElapsed(DLedgerEntryPusher.this.memberState.getAppliedIndex());
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }

                // clear the timeout pending closure which index > appliedIndex
                checkResponseFuturesTimeout(DLedgerEntryPusher.this.memberState.getAppliedIndex() + 1);

                // update peer watermarks of self
                updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());

                // calculate the median of watermarks(which we can ensure that more than half of the nodes have been pushed the corresponding entry)
                // we can also call it quorumIndex
                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                List<Long> sortedWaterMarks = peerWaterMarks.values()
                    .stream()
                    .sorted(Comparator.reverseOrder())
                    .collect(Collectors.toList());
                long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);

                // advance the commit index
                if (DLedgerEntryPusher.this.memberState.leaderUpdateCommittedIndex(currTerm, quorumIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(quorumIndex);
                } else {
                    // If the commit index is not advanced, we should wait for the next round
                    waitForRunning(1);
                }
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }

    /**
     * This thread will be activated by the leader.
     * This thread will push the entry to follower(identified by peerId) and update the completed pushed index to index map.
     * Should generate a single thread for each peer.
     * The push has 4 types:
     * APPEND : append the entries to the follower
     * COMPARE : if the leader changes, the new leader should compare its entries to follower's
     * TRUNCATE : if the leader finished comparing by an index, the leader will send a request to truncate the follower's ledger
     * COMMIT: usually, the leader will attach the committed index with the APPEND request, but if the append requests are few and scattered,
     * the leader will send a pure request to inform the follower of committed index.
     * <p>
     * The common transferring between these types are as following:
     * <p>
     * COMPARE ---- TRUNCATE ---- APPEND ---- COMMIT
     * ^                             |
     * |---<-----<------<-------<----|
     */
    private class EntryDispatcher extends ShutdownAbleThread {

        private final AtomicReference<EntryDispatcherState> type = new AtomicReference<>(EntryDispatcherState.COMPARE);
        private long lastPushCommitTimeMs = -1;
        private final String peerId;

        /**
         * the index of the next entry to push(initialized to the next of the last entry in the store)
         */
        private long writeIndex = DLedgerEntryPusher.this.dLedgerStore.getLedgerEndIndex() + 1;

        /**
         * the index of the last entry to be pushed to this peer(initialized to -1)
         */
        private long matchIndex = -1;

        private final int maxPendingSize = 1000;
        private long term = -1;
        private String leaderId = null;
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private final ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();
        private final ConcurrentMap<Long, Pair<Long, Integer>> batchPendingMap = new ConcurrentHashMap<>();
        private final PushEntryRequest batchAppendEntryRequest = new PushEntryRequest();
        private final Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        @Override
        public synchronized void start() {
            super.start();
            // initialize write index
            writeIndex = DLedgerEntryPusher.this.dLedgerStore.getLedgerEndIndex() + 1;
        }

        private boolean checkNotLeaderAndFreshState() {
            if (!memberState.isLeader()) {
                return true;
            }
            if (term != memberState.currTerm() || leaderId == null || !leaderId.equals(memberState.getLeaderId())) {
                synchronized (memberState) {
                    if (!memberState.isLeader()) {
                        return true;
                    }
                    PreConditions.check(memberState.getSelfId().equals(memberState.getLeaderId()), DLedgerResponseCode.UNKNOWN);
                    term = memberState.currTerm();
                    leaderId = memberState.getSelfId();
                    changeState(EntryDispatcherState.COMPARE);
                }
            }
            return false;
        }

        private PushEntryRequest buildPushRequest(long preLogTerm, long preLogIndex, PushEntryRequest.Type type) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setPreLogIndex(preLogIndex);
            request.setPreLogTerm(preLogTerm);
            request.setType(type);
            request.setCommitIndex(memberState.getCommittedIndex());
            return request;
        }

        private PushEntryRequest buildPushRequest(DLedgerEntry entry, PushEntryRequest.Type target) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setEntry(entry);
            request.setType(target);
            request.setCommitIndex(memberState.getCommittedIndex());
            return request;
        }

        private void resetBatchAppendEntryRequest() {
            batchAppendEntryRequest.setGroup(memberState.getGroup());
            batchAppendEntryRequest.setRemoteId(peerId);
            batchAppendEntryRequest.setLeaderId(leaderId);
            batchAppendEntryRequest.setLocalId(memberState.getSelfId());
            batchAppendEntryRequest.setTerm(term);
            batchAppendEntryRequest.setType(PushEntryRequest.Type.APPEND);
            batchAppendEntryRequest.clear();
        }

        private void checkQuotaAndWait(DLedgerEntry entry) {
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {
                long leftNow = quota.leftNow();
                logger.warn("[Push-{}]Quota exhaust, will sleep {}ms", peerId, leftNow);
                DLedgerUtils.sleep(leftNow);
            }
        }

        /**
         * append the entries to the follower
         *
         * @param index from which index to append
         * @return the index of the last entry to be appended
         * @throws Exception
         */
        private long doAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                // means should install snapshot
                changeState(EntryDispatcherState.INSTALL_SNAPSHOT);
                return -1;
            }
            checkQuotaAndWait(entry);
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
            pendingMap.put(index, System.currentTimeMillis());
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            pendingMap.remove(x.getIndex());
                            if (x.getIndex() > matchIndex) {
                                matchIndex = x.getIndex();
                            }
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            quorumAckChecker.wakeup();
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(EntryDispatcherState.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("Failed to deal with the callback when push request return", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
            return entry.getIndex();
        }

        private boolean doInstallSnapshot() throws Exception {
            // get snapshot from snapshot manager
//            if (!fsmCaller.isPresent() || fsmCaller.get().getSnapshotManager() == null) {
//                logger.error("[DoInstallSnapshot] snapshot mode is disabled");
//                return false;
//            }
//            SnapshotManager snapshotManager = fsmCaller.get().getSnapshotManager();
//            InstallSnapshotRequest request;
//            SnapshotReader snapshotReader = snapshotManager.getSnapshotReaderIncludedTargetIndex(index);
//            if (snapshotReader == null) {
//                logger.error("[DoInstallSnapshot] get latest snapshot whose lastIncludedIndex > {}  failed", index);
//                return false;
//            }
//            DownloadSnapshot snapshot = snapshotReader.generateDownloadSnapshot();
//            if (snapshot == null) {
//                logger.error("[DoInstallSnapshot] generate latest snapshot for download failed, index = {}", index);
//                return false;
//            }
//            request = new InstallSnapshotRequest(snapshot.getMeta().getLastIncludedIndex(), snapshot.getMeta().getLastIncludedTerm(), snapshot.getData());
//            CompletableFuture<InstallSnapshotResponse> future = DLedgerEntryPusher.this.dLedgerRpcService.installSnapshot(request);
//            future.
//            return true;
            logger.error("[DoInstallSnapshot] snapshot mode is disabled");
            return false;
        }

        private DLedgerEntry getDLedgerEntryForAppend(long index) {
            DLedgerEntry entry;
            try {
                entry = dLedgerStore.get(index);
            } catch (DLedgerException e) {
                //  Do compare, in case the ledgerBeginIndex get refreshed.
                if (DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN.equals(e.getCode())) {
                    logger.info("[Push-{}]Get INDEX_LESS_THAN_LOCAL_BEGIN when requested index is {}, try to compare", peerId, index);
                    return null;
                }
                throw e;
            }
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            return entry;
        }

        private void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                //Ignore the results
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        private void doCheckAppendResponse() throws Exception {
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLedgerConfig.getMaxPushTimeOutMs()) {
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1);
            }
        }

//        private void sendBatchAppendEntryRequest() throws Exception {
//            batchAppendEntryRequest.setCommitIndex(dLedgerStore.getCommittedIndex());
//            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
//            batchPendingMap.put(batchAppendEntryRequest.getFirstEntryIndex(), new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
//            responseFuture.whenComplete((x, ex) -> {
//                try {
//                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
//                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
//                    switch (responseCode) {
//                        case SUCCESS:
//                            batchPendingMap.remove(x.getIndex());
//                            if (x.getCount() == 0) {
//                                updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
//                            } else {
//                                updatePeerWaterMark(x.getTerm(), peerId, x.getIndex() + x.getCount() - 1);
//                            }
//                            break;
//                        case INCONSISTENT_STATE:
//                            logger.info("[Push-{}]Get INCONSISTENT_STATE when batch push index={} term={}", peerId, x.getIndex(), x.getTerm());
//                            changeState(-1, PushEntryRequest.Type.COMPARE);
//                            break;
//                        default:
//                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
//                            break;
//                    }
//                } catch (Throwable t) {
//                    logger.error("", t);
//                }
//            });
//            lastPushCommitTimeMs = System.currentTimeMillis();
//            batchAppendEntryRequest.clear();
//        }

//        private void doBatchAppendInner(long index) throws Exception {
//            DLedgerEntry entry = getDLedgerEntryForAppend(index);
//            if (null == entry) {
//                return;
//            }
//            batchAppendEntryRequest.addEntry(entry);
//            if (batchAppendEntryRequest.getTotalSize() >= dLedgerConfig.getMaxBatchPushSize()) {
//                sendBatchAppendEntryRequest();
//            }
//        }
//
//        private void doCheckBatchAppendResponse() throws Exception {
//            long peerWaterMark = getPeerWaterMark(term, peerId);
//            Pair<Long, Integer> pair = batchPendingMap.get(peerWaterMark + 1);
//            if (pair != null && System.currentTimeMillis() - pair.getKey() > dLedgerConfig.getMaxPushTimeOutMs()) {
//                long firstIndex = peerWaterMark + 1;
//                long lastIndex = firstIndex + pair.getValue() - 1;
//                logger.warn("[Push-{}]Retry to push entry from {} to {}", peerId, firstIndex, lastIndex);
//                batchAppendEntryRequest.clear();
//                for (long i = firstIndex; i <= lastIndex; i++) {
//                    DLedgerEntry entry = dLedgerStore.get(i);
//                    batchAppendEntryRequest.addEntry(entry);
//                }
//                sendBatchAppendEntryRequest();
//            }
//        }

//        private void doBatchAppend() throws Exception {
//            while (true) {
//                if (checkNotLeaderAndFreshState()) {
//                    break;
//                }
//                if (type.get() != PushEntryRequest.Type.APPEND) {
//                    break;
//                }
//                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
//                    if (batchAppendEntryRequest.getCount() > 0) {
//                        sendBatchAppendEntryRequest();
//                    }
//                    doCommit();
//                    doCheckBatchAppendResponse();
//                    break;
//                }
//                if (batchPendingMap.size() >= maxPendingSize || DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
//                    long peerWaterMark = getPeerWaterMark(term, peerId);
//                    for (Map.Entry<Long, Pair<Long, Integer>> entry : batchPendingMap.entrySet()) {
//                        if (entry.getKey() + entry.getValue().getValue() - 1 <= peerWaterMark) {
//                            batchPendingMap.remove(entry.getKey());
//                        }
//                    }
//                    lastCheckLeakTimeMs = System.currentTimeMillis();
//                }
//                if (batchPendingMap.size() >= maxPendingSize) {
//                    doCheckBatchAppendResponse();
//                    break;
//                }
//                doBatchAppendInner(writeIndex);
//                writeIndex++;
//            }
//        }

        private synchronized void changeState(EntryDispatcherState target) {
            logger.info("[Push-{}]Change state from {} to {}, matchIndex: {}, writeIndex: {}", peerId, type.get(), target, matchIndex, writeIndex);
//            switch (target) {
//                case APPEND:
//                    updatePeerWaterMark(term, peerId, index);
//                    quorumAckChecker.wakeup();
//                    writeIndex = index + 1;
//                    if (dLedgerConfig.isEnableBatchPush()) {
//                        resetBatchAppendEntryRequest();
//                    }
//                    break;
//                case COMPARE:
//                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
//                        compareIndex = -1;
//                        if (dLedgerConfig.isEnableBatchPush()) {
//                            batchPendingMap.clear();
//                        } else {
//                            pendingMap.clear();
//                        }
//                    }
//                    break;
//                case TRUNCATE:
//                default:
//                    break;
//            }
            type.set(target);
        }

        @Override
        public void doWork() {
            try {
                if (checkNotLeaderAndFreshState()) {
                    waitForRunning(1);
                    return;
                }
                switch (type.get()) {
                    case COMPARE:
                        doCompare();
                        break;
                    case TRUNCATE:
                        doTruncate();
                        break;
                    case APPEND:
                        doAppend();
                        break;
                    case INSTALL_SNAPSHOT:
                        doInstallSnapshot();
                        break;
                    case COMMIT:
                        doCommit();
                        break;
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("[Push-{}]Error in {} writeIndex={} matchIndex={}", peerId, getName(), writeIndex, matchIndex, t);
                changeState(EntryDispatcherState.COMPARE);
                DLedgerUtils.sleep(500);
            }
        }

        /**
         * First compare the leader store with the follower store, find the match index for the follower, and update write index to [matchIndex + 1]
         *
         * @throws Exception
         */
        private void doCompare() throws Exception {
            while (true) {
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                if (this.type.get() != EntryDispatcherState.COMPARE) {
                    break;
                }
                if (dLedgerStore.getLedgerEndIndex() == -1) {
                    // now not entry in store
                    break;
                }

                // compare process start from the [nextIndex -1]
                PushEntryRequest request;
                long compareIndex = writeIndex - 1;
                if (compareIndex == -1) {
                    request = buildPushRequest(-1, compareIndex, PushEntryRequest.Type.COMPARE);
                } else {
                    DLedgerEntry entry = dLedgerStore.get(compareIndex);
                    PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                    request = buildPushRequest(entry.getTerm(), entry.getIndex(), PushEntryRequest.Type.COMPARE);
                }
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode()
                    , DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);

                // fast backup algorithm to locate the match index
                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    // leader find the matched index for this follower
                    matchIndex = compareIndex;
                    // change state to truncate
                    changeState(EntryDispatcherState.TRUNCATE);
                    return;
                }

                // inconsistent state, need to keep comparing
                if (response.getXTerm() != -1) {
                    writeIndex = response.getXIndex();
                } else {
                    writeIndex = response.getEndIndex() + 1;
                }
            }
        }

        private void doTruncate() throws Exception {
            PreConditions.check(type.get() == EntryDispatcherState.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            // truncate all entries after truncateIndex for follower
            long truncateIndex = matchIndex + 1;
            logger.info("[Push-{}]Will push data to truncate truncateIndex={}", peerId, truncateIndex);
            PushEntryRequest truncateRequest = buildPushRequest(-1, truncateIndex, PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);

            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);
            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            changeState(EntryDispatcherState.APPEND);
        }

        private void doAppend() throws Exception {
            while (true) {
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                if (type.get() != EntryDispatcherState.APPEND) {
                    break;
                }
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    doCommit();
                    doCheckAppendResponse();
                    break;
                }
                if (pendingMap.size() >= maxPendingSize || DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Long index : pendingMap.keySet()) {
                        if (index < peerWaterMark) {
                            pendingMap.remove(index);
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (pendingMap.size() >= maxPendingSize) {
                    doCheckAppendResponse();
                    break;
                }
                long lastIndexToBeSend = doAppendInner(writeIndex);
                if (lastIndexToBeSend == -1) {
                    break;
                }
                writeIndex = lastIndexToBeSend + 1;
            }
        }

    }

    enum EntryDispatcherState {
        COMPARE,
        TRUNCATE,
        APPEND,
        INSTALL_SNAPSHOT,
        COMMIT
    }

    /**
     * This thread will be activated by the follower.
     * Accept the push request and order it by the index, then append to ledger store one by one.
     */
    private class EntryHandler extends ShutdownAbleThread {

        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>>
            compareOrTruncateRequests = new ArrayBlockingQueue<>(1024);

        public EntryHandler(Logger logger) {
            super("EntryHandler-" + memberState.getSelfId(), logger);
        }

        public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
            //The timeout should smaller than the remoting layer's request timeout
            CompletableFuture<PushEntryResponse> future = new TimeoutFuture<>(1000);
            switch (request.getType()) {
                case APPEND:
                    if (request.isBatch()) {
                        PreConditions.check(request.getBatchEntry() != null && request.getCount() > 0, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    } else {
                        PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    }
                    long index = request.getFirstEntryIndex();
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> old = writeRequestMap.putIfAbsent(index, new Pair<>(request, future));
                    if (old != null) {
                        logger.warn("[MONITOR]The index {} has already existed with {} and curr is {}", index, old.getKey().baseInfo(), request.baseInfo());
                        future.complete(buildResponse(request, DLedgerResponseCode.REPEATED_PUSH.getCode()));
                    }
                    break;
                case COMMIT:
                    synchronized (this) {
                        if (!compareOrTruncateRequests.offer(new Pair<>(request, future))) {
                            logger.warn("compareOrTruncateRequests blockingQueue is full when put commit request");
                            future.complete(buildResponse(request, DLedgerResponseCode.PUSH_REQUEST_IS_FULL.getCode()));
                        }
                    }
                    break;
                case COMPARE:
                case TRUNCATE:
                    writeRequestMap.clear();
                    synchronized (this) {
                        if (!compareOrTruncateRequests.offer(new Pair<>(request, future))) {
                            logger.warn("compareOrTruncateRequests blockingQueue is full when put compare or truncate request");
                            future.complete(buildResponse(request, DLedgerResponseCode.PUSH_REQUEST_IS_FULL.getCode()));
                        }
                    }
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            wakeup();
            return future;
        }

        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getFirstEntryIndex());
                response.setCount(request.getCount());
            }
//            response.setBeginIndex(dLedgerStore.getLedgerBeforeBeginIndex() + 1);
//            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        private void handleDoAppend(long writeIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                DLedgerEntry entry = dLedgerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                long committedIndex = Math.min(dLedgerStore.getLedgerEndIndex(), request.getCommitIndex());
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
            } catch (Throwable t) {
                logger.error("[HandleDoAppend] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        private CompletableFuture<PushEntryResponse> handleDoCompare(PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                // fast backup algorithm
                long preLogIndex = request.getPreLogIndex();
                long preLogTerm = request.getPreLogTerm();
                if (preLogTerm == -1 && preLogIndex == -1) {
                    // leader's entries is empty
                    future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                    return future;
                }
                if (dLedgerStore.getLedgerEndIndex() >= preLogIndex) {
                    // there exist a log whose index is preLogIndex
                    DLedgerEntry local = dLedgerStore.get(preLogIndex);
                    if (local.getTerm() == preLogTerm) {
                        // the log's term is preLogTerm
                        // all matched!
                        future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                        return future;
                    }
                    // if the log's term is not preLogTerm, we need to find the first log of this term
                    DLedgerEntry firstEntryWithTargetTerm = dLedgerStore.getFirstLogOfTargetTerm(local.getTerm(), preLogIndex);
                    PreConditions.check(firstEntryWithTargetTerm != null, DLedgerResponseCode.INCONSISTENT_STATE);
                    PushEntryResponse response = buildResponse(request, DLedgerResponseCode.INCONSISTENT_LEADER.getCode());
                    response.setXTerm(local.getTerm());
                    response.setXIndex(firstEntryWithTargetTerm.getIndex());
                    future.complete(response);
                    return future;
                }
                // if there doesn't exist entry in preLogIndex, we return last entry index
                PushEntryResponse response = buildResponse(request, DLedgerResponseCode.INCONSISTENT_LEADER.getCode());
                response.setEndIndex(dLedgerStore.getLedgerEndIndex());
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] preLogIndex={}, preLogTerm", request.getPreLogIndex(), request.getPreLogTerm(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                committedIndex = committedIndex <= dLedgerStore.getLedgerEndIndex() ? committedIndex : dLedgerStore.getLedgerEndIndex();
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request,
            CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={}", truncateIndex);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                long index = dLedgerStore.truncate(truncateIndex);
                PreConditions.check(index == truncateIndex - 1, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                long committedIndex = request.getCommitIndex() <= dLedgerStore.getLedgerEndIndex() ? request.getCommitIndex() : dLedgerStore.getLedgerEndIndex();
                if (DLedgerEntryPusher.this.memberState.followerUpdateCommittedIndex(committedIndex)) {
                    DLedgerEntryPusher.this.fsmCaller.onCommitted(committedIndex);
                }
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

//        private void handleDoBatchAppend(long writeIndex, PushEntryRequest request,
//            CompletableFuture<PushEntryResponse> future) {
//            try {
//                PreConditions.check(writeIndex == request.getFirstEntryIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
//                for (DLedgerEntry entry : request.getBatchEntry()) {
//                    dLedgerStore.appendAsFollower(entry, request.getTerm(), request.getLeaderId());
//                }
//                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
//                updateCommittedIndex(request.getTerm(), request.getCommitIndex());
//            } catch (Throwable t) {
//                logger.error("[HandleDoBatchAppend]", t);
//                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
//            }
//
//        }

        private void checkAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long firstEntryIndex = pair.getKey().getFirstEntryIndex();
                long lastEntryIndex = pair.getKey().getLastEntryIndex();
                // clear old push request
                if (lastEntryIndex <= endIndex) {
                    try {
                        if (pair.getKey().isBatch()) {
                            for (DLedgerEntry dLedgerEntry : pair.getKey().getBatchEntry()) {
                                PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                            }
                        } else {
                            DLedgerEntry dLedgerEntry = pair.getKey().getEntry();
                            PreConditions.check(dLedgerEntry.equals(dLedgerStore.get(dLedgerEntry.getIndex())), DLedgerResponseCode.INCONSISTENT_STATE);
                        }
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.SUCCESS.getCode()));
                        logger.warn("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex);
                    } catch (Throwable t) {
                        logger.error("[PushFallBehind]The leader pushed an batch append entry last index={} smaller than current ledgerEndIndex={}, maybe the last ack is missed", lastEntryIndex, endIndex, t);
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
                    }
                    writeRequestMap.remove(pair.getKey().getFirstEntryIndex());
                    continue;
                }
                // normal case
                if (firstEntryIndex == endIndex + 1) {
                    return;
                }
                // clear timeout push request
                TimeoutFuture<PushEntryResponse> future = (TimeoutFuture<PushEntryResponse>) pair.getValue();
                if (!future.isTimeOut()) {
                    continue;
                }
                if (firstEntryIndex < minFastForwardIndex) {
                    minFastForwardIndex = firstEntryIndex;
                }
            }
            if (minFastForwardIndex == Long.MAX_VALUE) {
                return;
            }
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(minFastForwardIndex);
            if (pair == null) {
                return;
            }
            logger.warn("[PushFastForward] ledgerEndIndex={} entryIndex={}", endIndex, minFastForwardIndex);
            pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
        }

        /**
         * The leader does push entries to follower, and record the pushed index. But in the following conditions, the push may get stopped.
         * * If the follower is abnormally shutdown, its ledger end index may be smaller than before. At this time, the leader may push fast-forward entries, and retry all the time.
         * * If the last ack is missed, and no new message is coming in.The leader may retry push the last message, but the follower will ignore it.
         *
         * @param endIndex
         */
        private void checkAbnormalFuture(long endIndex) {
            if (DLedgerUtils.elapsed(lastCheckFastForwardTimeMs) < 1000) {
                return;
            }
            lastCheckFastForwardTimeMs = System.currentTimeMillis();
            if (writeRequestMap.isEmpty()) {
                return;
            }

            checkAppendFuture(endIndex);
        }

        private void clearCompareOrTruncateRequestsIfNeed() {
            synchronized (this) {
                if (!memberState.isFollower() && !compareOrTruncateRequests.isEmpty()) {
                    List<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> drainList = new ArrayList<>();
                    compareOrTruncateRequests.drainTo(drainList);
                    for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : drainList) {
                        pair.getValue().complete(buildResponse(pair.getKey(), DLedgerResponseCode.NOT_FOLLOWER.getCode()));
                    }
                }
            }
        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isFollower()) {
                    clearCompareOrTruncateRequestsIfNeed();
                    waitForRunning(1);
                    return;
                }
                // deal with the compare or truncate requests first
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getPreLogIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                    return;
                }
                long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                if (pair == null) {
                    checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                    waitForRunning(1);
                    return;
                }
                PushEntryRequest request = pair.getKey();
//                if (request.isBatch()) {
//                    handleDoBatchAppend(nextIndex, request, pair.getValue());
//                } else {
//                    handleDoAppend(nextIndex, request, pair.getValue());
//                }
                handleDoAppend(nextIndex, request, pair.getValue());
            } catch (Throwable t) {
                DLedgerEntryPusher.LOGGER.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
        }
    }
}
