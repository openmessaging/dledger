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
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.ChangePeersRequest;
import io.openmessaging.storage.dledger.protocol.ChangePeersResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class DLedgerEntryPusher extends AbstractDLedgerEntryPusher {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerEntryPusher.class);

    private final Map<Long, ConcurrentMap<String, Long>> peerWaterMarksByTerm = new ConcurrentHashMap<>();
    private final Map<Long, ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>>> pendingAppendResponsesByTerm = new ConcurrentHashMap<>();

    private final EntryHandler entryHandler;

    private final QuorumAckChecker quorumAckChecker;

    private final Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();
    private final Map<String, LearnerEntryDispatcher> learnerDispatcherMap = new HashMap<>();

    private Optional<ChangePeersMark> changePeersMark;

    private Optional<StateMachineCaller> fsmCaller;


    public DLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
                              DLedgerRpcService dLedgerRpcService) {
        super(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);
        for (String peer : memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, LOGGER));
            }
        }
        for (String learner: memberState.getLearnerMap().keySet()) {
            learnerDispatcherMap.put(learner, new LearnerEntryDispatcher(learner, LOGGER));
        }
        this.entryHandler = new EntryHandler(LOGGER);
        this.quorumAckChecker = new QuorumAckChecker(LOGGER);
        this.fsmCaller = Optional.empty();
        this.changePeersMark = Optional.empty();
    }

    public void startup() {
        entryHandler.start();
        quorumAckChecker.start();
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.start();
        }
        for (LearnerEntryDispatcher dispatcher : learnerDispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (LearnerEntryDispatcher dispatcher : learnerDispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }

    private void checkTermForPendingMap(long term, String env) {
        if (!pendingAppendResponsesByTerm.containsKey(term)) {
            LOGGER.info("Initialize the pending append map in {} for term={}", env, term);
            pendingAppendResponsesByTerm.putIfAbsent(term, new ConcurrentHashMap<>());
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

    protected void checkTermForWaterMark(long term, String env) {
        if (!peerWaterMarksByTerm.containsKey(term)) {
            LOGGER.info("Initialize the watermark in {} for term={}", env, term);
            ConcurrentMap<String, Long> waterMarks = new ConcurrentHashMap<>();
            for (String peer : memberState.getPeerMap().keySet()) {
                waterMarks.put(peer, -1L);
            }
            peerWaterMarksByTerm.putIfAbsent(term, waterMarks);
        }
    }



    public long getPeerWaterMark(long term, String peerId) {
        synchronized (peerWaterMarksByTerm) {
            checkTermForWaterMark(term, "getPeerWaterMark");
            return peerWaterMarksByTerm.get(term).get(peerId);
        }
    }

    @Override
    public boolean checkSelfIsNotLearner(String groupId, String peerId) {
            return !dLedgerConfig.getLearnerAddressMap().containsKey(DLedgerUtils.generateDLedgerId(groupId, peerId));
    }

    public boolean isPendingFull(long currTerm) {
        checkTermForPendingMap(currTerm, "isPendingFull");
        return pendingAppendResponsesByTerm.get(currTerm).size() > dLedgerConfig.getMaxPendingRequestsNum();
    }

    public CompletableFuture<ChangePeersResponse> notifyChangePeers(ChangePeersRequest request, DLedgerEntry entry) {
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        checkTermForPendingMap(entry.getTerm(), "waitAck");
        AppendFuture<AppendEntryResponse> future;
        future = new AppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
        future.setPos(entry.getPos());
        CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
        if (old != null) {
            LOGGER.warn("[MONITOR] get old wait at index={}", entry.getIndex());
        }

        ChangePeersMark changePeersMark = new ChangePeersMark(entry.getTerm(), entry.getIndex(),
                request.getAddPeers(),request.getRemovePeers());
        this.changePeersMark = Optional.of(changePeersMark);

        ChangePeersFuture<ChangePeersResponse> changePeersResponseFuture = new ChangePeersFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
        future.whenCompleteAsync((x,y)-> {
            future.complete(x);
        });
        return changePeersResponseFuture;
    }

    public CompletableFuture<AppendEntryResponse> waitAck(DLedgerEntry entry, boolean isBatchWait) {
        updatePeerWaterMark(entry.getTerm(), memberState.getSelfId(), entry.getIndex());
        checkTermForPendingMap(entry.getTerm(), "waitAck");
        AppendFuture<AppendEntryResponse> future;
        if (isBatchWait) {
            future = new BatchAppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
        } else {
            future = new AppendFuture<>(dLedgerConfig.getMaxWaitAckTimeMs());
        }
        future.setPos(entry.getPos());
        CompletableFuture<AppendEntryResponse> old = pendingAppendResponsesByTerm.get(entry.getTerm()).put(entry.getIndex(), future);
        if (old != null) {
            LOGGER.warn("[MONITOR] get old wait at index={}", entry.getIndex());
        }
        return future;
    }


    /**
     * Complete the TimeoutFuture in pendingAppendResponsesByTerm (CurrentTerm, index).
     * Called by statemachineCaller when a committed entry (CurrentTerm, index) was applying to statemachine done.
     *
     * @return true if complete success
     */
    public boolean completeResponseFuture(final long index) {
        final long term = this.memberState.currTerm();
        final Map<Long, TimeoutFuture<AppendEntryResponse>> responses = this.pendingAppendResponsesByTerm.get(term);
        if (responses != null) {
            CompletableFuture<AppendEntryResponse> future = responses.remove(index);
            if (future != null && !future.isDone()) {
                LOGGER.info("Complete future, term {}, index {}", term, index);
                AppendEntryResponse response = new AppendEntryResponse();
                response.setGroup(this.memberState.getGroup());
                response.setTerm(term);
                response.setIndex(index);
                response.setLeaderId(this.memberState.getSelfId());
                response.setPos(((AppendFuture<?>) future).getPos());
                future.complete(response);
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
        final Map<Long, TimeoutFuture<AppendEntryResponse>> responses = this.pendingAppendResponsesByTerm.get(term);
        if (responses != null) {
            for (long i = beginIndex; i < Integer.MAX_VALUE; i++) {
                TimeoutFuture<AppendEntryResponse> future = responses.get(i);
                if (future == null) {
                    break;
                } else if (future.isTimeOut()) {
                    AppendEntryResponse response = new AppendEntryResponse();
                    response.setGroup(memberState.getGroup());
                    response.setCode(DLedgerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode());
                    response.setTerm(term);
                    response.setIndex(i);
                    response.setLeaderId(memberState.getSelfId());
                    future.complete(response);
                } else {
                    break;
                }
            }
        }
    }

    /**
     * Check responseFutures elapsed before {endIndex} in currentTerm
     */
    protected void checkResponseFuturesElapsed(final long endIndex) {
        final long currTerm = this.memberState.currTerm();
        final Map<Long, TimeoutFuture<AppendEntryResponse>> responses = this.pendingAppendResponsesByTerm.get(currTerm);
        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : responses.entrySet()) {
            if (futureEntry.getKey() <= endIndex) {
                AppendEntryResponse response = new AppendEntryResponse();
                response.setGroup(memberState.getGroup());
                response.setTerm(currTerm);
                response.setIndex(futureEntry.getKey());
                response.setLeaderId(memberState.getSelfId());
                response.setPos(((AppendFuture) futureEntry.getValue()).getPos());
                futureEntry.getValue().complete(response);
                responses.remove(futureEntry.getKey());
            }
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher : dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }

    /**
     * This thread will check the quorum index and complete the pending requests.
     */
    private class QuorumAckChecker extends ShutdownAbleThread {

        private long lastPrintWatermarkTimeMs = System.currentTimeMillis();
        private long lastCheckLeakTimeMs = System.currentTimeMillis();
        private long lastQuorumIndex = -1;

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker-" + memberState.getSelfId(), logger);
        }

        public String getLearnerCommittedIndex() {
            StringBuilder builder = new StringBuilder("{");
            for (LearnerEntryDispatcher dispatcher : learnerDispatcherMap.values()) {
                builder.append("\"peerId\":");
                builder.append("\"");
                builder.append(dispatcher.peerId);
                builder.append("\"");
                builder.append(",");
                builder.append("\"committedIndex\":");
                builder.append(dispatcher.committedIndex);
            }
            builder.append("}");
            return builder.toString();
        }
        @Override
        public void doWork() {
            try {
                if (DLedgerUtils.elapsed(lastPrintWatermarkTimeMs) > 3000) {
                    if (DLedgerEntryPusher.this.fsmCaller.isPresent()) {
                        final long lastAppliedIndex = DLedgerEntryPusher.this.fsmCaller.get().getLastAppliedIndex();
                        if (memberState.isLeader()) {
                            logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} learnerCommittedIndex= {} watermarks={} appliedIndex={}",
                                    memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), getLearnerCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm), lastAppliedIndex);
                        } else {
                            logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={} appliedIndex={}",
                                    memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm), lastAppliedIndex);
                        }
                    } else {
                        if (memberState.isLeader()) {
                            logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} learnerCommittedIndex= {} watermarks={}",
                                    memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), getLearnerCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                        } else {
                            logger.info("[{}][{}] term={} ledgerBegin={} ledgerEnd={} committed={} watermarks={}",
                                    memberState.getSelfId(), memberState.getRole(), memberState.currTerm(), dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex(), dLedgerStore.getCommittedIndex(), JSON.toJSONString(peerWaterMarksByTerm));
                        }
                    }
                    lastPrintWatermarkTimeMs = System.currentTimeMillis();
                }
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }
                long currTerm = memberState.currTerm();
                checkTermForPendingMap(currTerm, "QuorumAckChecker");
                checkTermForWaterMark(currTerm, "QuorumAckChecker");
                if (pendingAppendResponsesByTerm.size() > 1) {
                    for (Long term : pendingAppendResponsesByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        for (Map.Entry<Long, TimeoutFuture<AppendEntryResponse>> futureEntry : pendingAppendResponsesByTerm.get(term).entrySet()) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setGroup(memberState.getGroup());
                            response.setIndex(futureEntry.getKey());
                            response.setCode(DLedgerResponseCode.TERM_CHANGED.getCode());
                            response.setLeaderId(memberState.getLeaderId());
                            logger.info("[TermChange] Will clear the pending response index={} for term changed from {} to {}", futureEntry.getKey(), term, currTerm);
                            futureEntry.getValue().complete(response);
                        }
                        pendingAppendResponsesByTerm.remove(term);
                    }
                }
                if (peerWaterMarksByTerm.size() > 1) {
                    for (Long term : peerWaterMarksByTerm.keySet()) {
                        if (term == currTerm) {
                            continue;
                        }
                        logger.info("[TermChange] Will clear the watermarks for term changed from {} to {}", term, currTerm);
                        peerWaterMarksByTerm.remove(term);
                    }
                }

                Map<String, Long> peerWaterMarks = peerWaterMarksByTerm.get(currTerm);
                List<Long> sortedWaterMarks = peerWaterMarks.values()
                        .stream()
                        .sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList());
                long quorumIndex = sortedWaterMarks.get(sortedWaterMarks.size() / 2);
                final Optional<StateMachineCaller> fsmCaller = DLedgerEntryPusher.this.fsmCaller;
                if (fsmCaller.isPresent()) {
                    // If there exist statemachine
                    DLedgerEntryPusher.this.dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);
                    final StateMachineCaller caller = fsmCaller.get();
                    caller.onCommitted(quorumIndex);

                    // Check elapsed
                    if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) {
                        updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                        checkResponseFuturesElapsed(caller.getLastAppliedIndex());
                        lastCheckLeakTimeMs = System.currentTimeMillis();
                    }

                    if (quorumIndex == this.lastQuorumIndex) {
                        waitForRunning(1);
                    }
                } else {
                    dLedgerStore.updateCommittedIndex(currTerm, quorumIndex);
                    ConcurrentMap<Long, TimeoutFuture<AppendEntryResponse>> responses = pendingAppendResponsesByTerm.get(currTerm);
                    boolean needCheck = false;
                    int ackNum = 0;
                    for (long i = quorumIndex; i > lastQuorumIndex; i--) {
                        try {
                            CompletableFuture<AppendEntryResponse> future = responses.remove(i);
                            if (future == null) {
                                needCheck = true;
                                break;
                            } else if (!future.isDone()) {
                                AppendEntryResponse response = new AppendEntryResponse();
                                response.setGroup(memberState.getGroup());
                                response.setTerm(currTerm);
                                response.setIndex(i);
                                response.setLeaderId(memberState.getSelfId());
                                response.setPos(((AppendFuture) future).getPos());
                                future.complete(response);
                            }
                            ackNum++;
                        } catch (Throwable t) {
                            logger.error("Error in ack to index={} term={}", i, currTerm, t);
                        }
                    }

                    if (ackNum == 0) {
                        checkResponseFuturesTimeout(quorumIndex + 1);
                        waitForRunning(1);
                    }

                    if (DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000 || needCheck) {
                        updatePeerWaterMark(currTerm, memberState.getSelfId(), dLedgerStore.getLedgerEndIndex());
                        checkResponseFuturesElapsed(quorumIndex);
                        lastCheckLeakTimeMs = System.currentTimeMillis();
                    }
                }
                lastQuorumIndex = quorumIndex;
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
    private class EntryDispatcher extends AbstractEntryDispatcher {

        public EntryDispatcher(String peerId, Logger logger) {
            super(peerId, logger);
        }

        protected void doAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
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
                            updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            quorumAckChecker.wakeup();
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }

        protected void sendBatchAppendEntryRequest() throws Exception {
            batchAppendEntryRequest.setCommitIndex(dLedgerStore.getCommittedIndex());
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
            batchPendingMap.put(batchAppendEntryRequest.getFirstEntryIndex(), new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            batchPendingMap.remove(x.getIndex());
                            if (x.getCount() == 0) {
                                updatePeerWaterMark(x.getTerm(), peerId, x.getIndex());
                            } else {
                                updatePeerWaterMark(x.getTerm(), peerId, x.getIndex() + x.getCount() - 1);
                            }
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when batch push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
            batchAppendEntryRequest.clear();
        }


        public synchronized void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                case APPEND:
                    compareIndex = -1;
                    updatePeerWaterMark(term, peerId, index);
                    quorumAckChecker.wakeup();
                    writeIndex = index + 1;
                    if (dLedgerConfig.isEnableBatchPush()) {
                        resetBatchAppendEntryRequest();
                    }
                    break;
                case COMPARE:
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        compareIndex = -1;
                        if (dLedgerConfig.isEnableBatchPush()) {
                            batchPendingMap.clear();
                        } else {
                            pendingMap.clear();
                        }
                    }
                    break;
                case TRUNCATE:
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        protected void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                //Ignore the results
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }
    }


    class LearnerEntryDispatcher extends AbstractEntryDispatcher {

        private long committedIndex = -1;

        public LearnerEntryDispatcher(String peerId, Logger logger) {
            super(peerId, logger);
        }

        @Override
        protected void changeState(long index, PushEntryRequest.Type target) {
            logger.info("[Push-{}]Learner Change state from {} to {} at {}", peerId, type.get(), target, index);
            switch (target) {
                case APPEND:
                    compareIndex = -1;
                    writeIndex = index + 1;
                    if (dLedgerConfig.isEnableBatchPush()) {
                        resetBatchAppendEntryRequest();
                    }
                    break;
                case COMPARE:
                    if (this.type.compareAndSet(PushEntryRequest.Type.APPEND, PushEntryRequest.Type.COMPARE)) {
                        compareIndex = -1;
                        if (dLedgerConfig.isEnableBatchPush()) {
                            batchPendingMap.clear();
                        } else {
                            pendingMap.clear();
                        }
                    }
                    break;
                case TRUNCATE:
                    compareIndex = -1;
                    break;
                default:
                    break;
            }
            type.set(target);
        }

        protected void doCommit() throws Exception {
            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
                request.setCommitIndex(committedIndex);
                //Ignore the results
                dLedgerRpcService.push(request);
                lastPushCommitTimeMs = System.currentTimeMillis();
            }
        }

        public void updateCommitIndex(long index) {
            if (index > this.committedIndex) {
                this.committedIndex = index;
            } else {
                logger.warn("index: [{}] update error, committedIndex: [{}]", index, committedIndex);
            }
        }

        protected void sendBatchAppendEntryRequest() throws Exception {
            batchAppendEntryRequest.setCommitIndex(committedIndex);
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            updateCommitIndex(x.getIndex() + x.getCount() - 1);
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when batch push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                            break;
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                            break;
                    }
                } catch (Throwable t) {
                    logger.error("", t);
                }
            });
        }


        protected void doAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
            }
            checkQuotaAndWait(entry);
            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
            responseFuture.whenComplete((x, ex) -> {
                try {
                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
                    switch (responseCode) {
                        case SUCCESS:
                            updateCommitIndex(x.getIndex());
                            break;
                        case INCONSISTENT_STATE:
                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
                            changeState(-1, PushEntryRequest.Type.COMPARE);
                        default:
                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
                    }
                } catch (Throwable t) {
                    logger.error("append error, index: {}", index, t);
                }
            });
            lastPushCommitTimeMs = System.currentTimeMillis();
        }
    }
}
