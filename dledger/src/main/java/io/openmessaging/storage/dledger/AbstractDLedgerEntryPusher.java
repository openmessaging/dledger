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
/*
    create qiangzhiwei time 2022/12/17
 */

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.Pair;
import io.openmessaging.storage.dledger.utils.PreConditions;
import io.openmessaging.storage.dledger.utils.Quota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractDLedgerEntryPusher {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDLedgerEntryPusher.class);

    protected final DLedgerConfig dLedgerConfig;
    protected final DLedgerStore dLedgerStore;

    protected final MemberState memberState;

    protected final DLedgerRpcService dLedgerRpcService;


    private Optional<ChangePeersMark> changePeersMark;

    private Optional<StateMachineCaller> fsmCaller;

    public AbstractDLedgerEntryPusher(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore, DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = memberState;
        this.dLedgerStore = dLedgerStore;
        this.dLedgerRpcService = dLedgerRpcService;
        this.fsmCaller = Optional.empty();
        this.changePeersMark = Optional.empty();
    }



    public void startup() {
    }

    public void shutdown() {
    }

    public abstract long getPeerWaterMark(long term, String peerId);
    public abstract boolean checkSelfIsNotLearner(String groupId, String peerId);

    public void registerStateMachine(final Optional<StateMachineCaller> fsmCaller) {
        this.fsmCaller = fsmCaller;
    }


    protected void updateCommittedIndex(final long term, final long committedIndex) {
        dLedgerStore.updateCommittedIndex(term, committedIndex);
        this.fsmCaller.ifPresent(caller -> caller.onCommitted(committedIndex));
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
    protected abstract class AbstractEntryDispatcher extends ShutdownAbleThread {

        protected final AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.COMPARE);
        protected long lastPushCommitTimeMs = -1;
        protected final String peerId;
        protected long compareIndex = -1;
        protected final int maxPendingSize = 1000;
        protected long term = -1;
        protected String leaderId = null;
        protected final Quota quota = new Quota(dLedgerConfig.getPeerPushQuota());
        protected final ConcurrentMap<Long, Long> pendingMap = new ConcurrentHashMap<>();
        protected final ConcurrentMap<Long, Pair<Long, Integer>> batchPendingMap = new ConcurrentHashMap<>();
        protected long lastCheckLeakTimeMs = System.currentTimeMillis();
        protected long writeIndex = -1;
        protected final PushEntryRequest batchAppendEntryRequest = new PushEntryRequest();

        public AbstractEntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + memberState.getSelfId() + "-" + peerId, logger);
            this.peerId = peerId;
        }

        protected abstract void doAppendInner(long index) throws Exception;

        protected abstract void doCommit() throws Exception;

        protected abstract void changeState(long index, PushEntryRequest.Type target);

        protected abstract void sendBatchAppendEntryRequest() throws Exception;

        protected void resetBatchAppendEntryRequest() {
            batchAppendEntryRequest.setGroup(memberState.getGroup());
            batchAppendEntryRequest.setRemoteId(peerId);
            batchAppendEntryRequest.setLeaderId(leaderId);
            batchAppendEntryRequest.setLocalId(memberState.getSelfId());
            batchAppendEntryRequest.setTerm(term);
            batchAppendEntryRequest.setType(PushEntryRequest.Type.APPEND);
            batchAppendEntryRequest.clear();
        }

        protected boolean checkNotLeaderAndFreshState() {
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
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                }
            }
            return false;
        }

        protected PushEntryRequest buildPushRequest(DLedgerEntry entry, PushEntryRequest.Type target) {
            PushEntryRequest request = new PushEntryRequest();
            request.setGroup(memberState.getGroup());
            request.setRemoteId(peerId);
            request.setLeaderId(leaderId);
            request.setLocalId(memberState.getSelfId());
            request.setTerm(term);
            request.setEntry(entry);
            request.setType(target);
            request.setCommitIndex(dLedgerStore.getCommittedIndex());
            return request;
        }

        protected void checkQuotaAndWait(DLedgerEntry entry) {
            if (dLedgerStore.getLedgerEndIndex() - entry.getIndex() <= maxPendingSize) {
                return;
            }
            if (dLedgerStore instanceof DLedgerMemoryStore) {
                return;
            }
            DLedgerMmapFileStore mmapFileStore = (DLedgerMmapFileStore) dLedgerStore;
            if (mmapFileStore.getDataFileList().getMaxWrotePosition() - entry.getPos() < dLedgerConfig.getPeerPushThrottlePoint()) {
                return;
            }
            quota.sample(entry.getSize());
            if (quota.validateNow()) {
                long leftNow = quota.leftNow();
                logger.warn("[Push-{}]Quota exhaust, will sleep {}ms", peerId, leftNow);
                DLedgerUtils.sleep(leftNow);
            }
        }

        protected DLedgerEntry getDLedgerEntryForAppend(long index) {
            DLedgerEntry entry;
            try {
                entry = dLedgerStore.get(index);
            } catch (DLedgerException e) {
                //  Do compare, in case the ledgerBeginIndex get refreshed.
                if (DLedgerResponseCode.INDEX_LESS_THAN_LOCAL_BEGIN.equals(e.getCode())) {
                    logger.info("[Push-{}]Get INDEX_LESS_THAN_LOCAL_BEGIN when requested index is {}, try to compare", peerId, index);
                    changeState(-1, PushEntryRequest.Type.COMPARE);
                    return null;
                }
                throw e;
            }
            PreConditions.check(entry != null, DLedgerResponseCode.UNKNOWN, "writeIndex=%d", index);
            return entry;
        }

        protected void doAppend() throws Exception {
            while (true) {

                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }

                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    doCommit();
                    doCheckAppendResponse();
                    break;
                }
                if ( (pendingMap.size() >= maxPendingSize || DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) && checkSelfIsNotLearner(memberState.getGroup(), peerId)) {
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
                doAppendInner(writeIndex);
                writeIndex++;
            }
        }



        protected void doCheckAppendResponse() throws Exception {
            if (memberState.isLearner()) {
                return;
            }
            if (!checkSelfIsNotLearner(memberState.getGroup(), peerId)) {
                return;
            }
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Long sendTimeMs = pendingMap.get(peerWaterMark + 1);
            if (sendTimeMs != null && System.currentTimeMillis() - sendTimeMs > dLedgerConfig.getMaxPushTimeOutMs()) {
                logger.warn("[Push-{}]Retry to push entry at {}", peerId, peerWaterMark + 1);
                doAppendInner(peerWaterMark + 1);
            }
        }

        protected void doTruncate(long truncateIndex) throws Exception {
            PreConditions.check(type.get() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
            DLedgerEntry truncateEntry = dLedgerStore.get(truncateIndex);
            PreConditions.check(truncateEntry != null, DLedgerResponseCode.UNKNOWN);

            logger.info("[Push-{}]Will push data to truncate truncateIndex={} pos={}", peerId, truncateIndex, truncateEntry.getPos());
            PushEntryRequest truncateRequest = buildPushRequest(truncateEntry, PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse truncateResponse = dLedgerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            PreConditions.check(truncateResponse != null, DLedgerResponseCode.UNKNOWN, "truncateIndex=%d", truncateIndex);

            PreConditions.check(truncateResponse.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(truncateResponse.getCode()), "truncateIndex=%d", truncateIndex);
            lastPushCommitTimeMs = System.currentTimeMillis();
            changeState(truncateIndex, PushEntryRequest.Type.APPEND);
        }



        protected void doCompare() throws Exception {
            while (true) {
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.COMPARE && type.get() != PushEntryRequest.Type.TRUNCATE) {
                    break;
                }
                if (compareIndex == -1 && dLedgerStore.getLedgerEndIndex() == -1) {
                    break;
                }
                //revise the compareIndex
                if (compareIndex == -1) {
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                    logger.info("[Push-{}][DoCompare] compareIndex=-1 means start to compare", peerId);
                } else if (compareIndex > dLedgerStore.getLedgerEndIndex() || compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    logger.info("[Push-{}][DoCompare] compareIndex={} out of range {}-{}", peerId, compareIndex, dLedgerStore.getLedgerBeginIndex(), dLedgerStore.getLedgerEndIndex());
                    compareIndex = dLedgerStore.getLedgerEndIndex();
                }

                DLedgerEntry entry = dLedgerStore.get(compareIndex);
                PreConditions.check(entry != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.COMPARE);
                CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLedgerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);

                PreConditions.check(response.getCode() == DLedgerResponseCode.INCONSISTENT_STATE.getCode() || response.getCode() == DLedgerResponseCode.SUCCESS.getCode(), DLedgerResponseCode.valueOf(response.getCode()), "compareIndex=%d", compareIndex);
                long truncateIndex = -1;

                if (response.getCode() == DLedgerResponseCode.SUCCESS.getCode()) {
                    /*
                     * The comparison is successful:
                     * 1.Just change to append state, if the follower's end index is equal the compared index.
                     * 2.Truncate the follower, if the follower has some dirty entries.
                     */
                    if (compareIndex == response.getEndIndex()) {
                        changeState(compareIndex, PushEntryRequest.Type.APPEND);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < dLedgerStore.getLedgerBeginIndex() || response.getBeginIndex() > dLedgerStore.getLedgerEndIndex()) {
                    /*
                     The follower's entries does not intersect with the leader.
                     This usually happened when the follower has crashed for a long time while the leader has deleted the expired entries.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex < response.getBeginIndex()) {
                    /*
                     The compared index is smaller than the follower's begin index.
                     This happened rarely, usually means some disk damage.
                     Just truncate the follower.
                     */
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                } else if (compareIndex > response.getEndIndex()) {
                    /*
                     The compared index is bigger than the follower's end index.
                     This happened frequently. For the compared index is usually starting from the end index of the leader.
                     */
                    compareIndex = response.getEndIndex();
                } else {
                    /*
                      Compare failed and the compared index is in the range of follower's entries.
                     */
                    compareIndex--;
                }
                /*
                 The compared index is smaller than the leader's begin index, truncate the follower.
                 */
                if (compareIndex < dLedgerStore.getLedgerBeginIndex()) {
                    truncateIndex = dLedgerStore.getLedgerBeginIndex();
                }
                /*
                 If get value for truncateIndex, do it right now.
                 */
                if (truncateIndex != -1) {
                    changeState(truncateIndex, PushEntryRequest.Type.TRUNCATE);
                    doTruncate(truncateIndex);
                    break;
                }
            }
        }

        @Override
        public void doWork() {
            try {
                if (checkNotLeaderAndFreshState()) {
                    waitForRunning(1);
                    return;
                }
                if (type.get() == PushEntryRequest.Type.APPEND) {
                    if (dLedgerConfig.isEnableBatchPush()) {
                        doBatchAppend();
                    } else {
                        doAppend();
                    }
                } else {
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                LOGGER.error("[Push-{}]Error in {} writeIndex={} compareIndex={}", peerId, getName(), writeIndex, compareIndex, t);
                changeState(-1, PushEntryRequest.Type.COMPARE);
                DLedgerUtils.sleep(500);
            }
        }

        protected void doBatchAppend() throws Exception {
            while (true) {
                if (checkNotLeaderAndFreshState()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.APPEND) {
                    break;
                }
                if (writeIndex > dLedgerStore.getLedgerEndIndex()) {
                    if (batchAppendEntryRequest.getCount() > 0) {
                        sendBatchAppendEntryRequest();
                    }
                    doCommit();
                    doCheckBatchAppendResponse();
                    break;
                }
                if ((batchPendingMap.size() >= maxPendingSize || DLedgerUtils.elapsed(lastCheckLeakTimeMs) > 1000) && checkSelfIsNotLearner(memberState.getGroup(), peerId)) {
                    long peerWaterMark = getPeerWaterMark(term, peerId);
                    for (Map.Entry<Long, Pair<Long, Integer>> entry : batchPendingMap.entrySet()) {
                        if (entry.getKey() + entry.getValue().getValue() - 1 <= peerWaterMark) {
                            batchPendingMap.remove(entry.getKey());
                        }
                    }
                    lastCheckLeakTimeMs = System.currentTimeMillis();
                }
                if (batchPendingMap.size() >= maxPendingSize) {
                    doCheckBatchAppendResponse();
                    break;
                }
                doBatchAppendInner(writeIndex);
                writeIndex++;
            }


        }


        protected void doBatchAppendInner(long index) throws Exception {
            DLedgerEntry entry = getDLedgerEntryForAppend(index);
            if (null == entry) {
                return;
            }
            batchAppendEntryRequest.addEntry(entry);
            if (batchAppendEntryRequest.getTotalSize() >= dLedgerConfig.getMaxBatchPushSize()) {
                sendBatchAppendEntryRequest();
            }
        }

        protected void doCheckBatchAppendResponse() throws Exception {
            if (!checkSelfIsNotLearner(memberState.getGroup(), peerId)) {
                return;
            }
            long peerWaterMark = getPeerWaterMark(term, peerId);
            Pair<Long, Integer> pair = batchPendingMap.get(peerWaterMark + 1);
            if (pair != null && System.currentTimeMillis() - pair.getKey() > dLedgerConfig.getMaxPushTimeOutMs()) {
                long firstIndex = peerWaterMark + 1;
                long lastIndex = firstIndex + pair.getValue() - 1;
                logger.warn("[Push-{}]Retry to push entry from {} to {}", peerId, firstIndex, lastIndex);
                batchAppendEntryRequest.clear();
                for (long i = firstIndex; i <= lastIndex; i++) {
                    DLedgerEntry entry = dLedgerStore.get(i);
                    batchAppendEntryRequest.addEntry(entry);
                }
                sendBatchAppendEntryRequest();
            }
        }
    }


    protected class EntryHandler extends ShutdownAbleThread {

        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> writeRequestMap = new ConcurrentHashMap<>();
        BlockingQueue<Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> compareOrTruncateRequests = new ArrayBlockingQueue<>(100);

        public EntryHandler(Logger logger) {
            super("EntryHandler-" + memberState.getSelfId(), logger);
        }

        private long lastCheckFastForwardTimeMs = System.currentTimeMillis();

        private void handleDoAppend(long writeIndex, PushEntryRequest request,
                                    CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                DLedgerEntry entry = dLedgerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(entry.getIndex() == writeIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }



        private CompletableFuture<PushEntryResponse> handleDoCommit(long committedIndex, PushEntryRequest request,
                                                                    CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(committedIndex == request.getCommitIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMMIT, DLedgerResponseCode.UNKNOWN);
                updateCommittedIndex(request.getTerm(), committedIndex);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCommit] committedIndex={}", request.getCommitIndex(), t);
                future.complete(buildResponse(request, DLedgerResponseCode.UNKNOWN.getCode()));
            }
            return future;
        }

        private void checkAppendFuture(long endIndex) {
            long minFastForwardIndex = Long.MAX_VALUE;
            for (Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair : writeRequestMap.values()) {
                long firstEntryIndex = pair.getKey().getFirstEntryIndex();
                long lastEntryIndex = pair.getKey().getLastEntryIndex();
                //Fall behind
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
                if (firstEntryIndex == endIndex + 1) {
                    return;
                }
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
            Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.get(minFastForwardIndex);
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

        @Override
        public void doWork() {
            try {
                if (!(memberState.isFollower() || memberState.isLearner())) {
                    waitForRunning(1);
                    return;
                }
                if (compareOrTruncateRequests.peek() != null) {
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = compareOrTruncateRequests.poll();
                    PreConditions.check(pair != null, DLedgerResponseCode.UNKNOWN);
                    switch (pair.getKey().getType()) {
                        case TRUNCATE:
                            handleDoTruncate(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMPARE:
                            handleDoCompare(pair.getKey().getEntry().getIndex(), pair.getKey(), pair.getValue());
                            break;
                        case COMMIT:
                            handleDoCommit(pair.getKey().getCommitIndex(), pair.getKey(), pair.getValue());
                            break;
                        default:
                            break;
                    }
                } else {
                    long nextIndex = dLedgerStore.getLedgerEndIndex() + 1;
                    Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair = writeRequestMap.remove(nextIndex);
                    if (pair == null) {
                        checkAbnormalFuture(dLedgerStore.getLedgerEndIndex());
                        waitForRunning(1);
                        return;
                    }
                    PushEntryRequest request = pair.getKey();
                    if (request.isBatch()) {
                        handleDoBatchAppend(nextIndex, request, pair.getValue());
                    } else {
                        handleDoAppend(nextIndex, request, pair.getValue());
                    }
                }
            } catch (Throwable t) {
                LOGGER.error("Error in {}", getName(), t);
                DLedgerUtils.sleep(100);
            }
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
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                case COMPARE:
                case TRUNCATE:
                    PreConditions.check(request.getEntry() != null, DLedgerResponseCode.UNEXPECTED_ARGUMENT);
                    writeRequestMap.clear();
                    compareOrTruncateRequests.put(new Pair<>(request, future));
                    break;
                default:
                    logger.error("[BUG]Unknown type {} from {}", request.getType(), request.baseInfo());
                    future.complete(buildResponse(request, DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
                    break;
            }
            wakeup();
            return future;
        }

        protected PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setGroup(request.getGroup());
            response.setCode(code);
            response.setTerm(request.getTerm());
            if (request.getType() != PushEntryRequest.Type.COMMIT) {
                response.setIndex(request.getFirstEntryIndex());
                response.setCount(request.getCount());
            }
            response.setBeginIndex(dLedgerStore.getLedgerBeginIndex());
            response.setEndIndex(dLedgerStore.getLedgerEndIndex());
            return response;
        }

        protected CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(compareIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.COMPARE, DLedgerResponseCode.UNKNOWN);
                DLedgerEntry local = dLedgerStore.get(compareIndex);
                PreConditions.check(request.getEntry().equals(local), DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }


        protected CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                logger.info("[HandleDoTruncate] truncateIndex={} pos={}", truncateIndex, request.getEntry().getPos());
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLedgerResponseCode.UNKNOWN);
                PreConditions.check(request.getType() == PushEntryRequest.Type.TRUNCATE, DLedgerResponseCode.UNKNOWN);
                long index = dLedgerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLedgerResponseCode.INCONSISTENT_STATE);
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private void handleDoBatchAppend(long writeIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getFirstEntryIndex(), DLedgerResponseCode.INCONSISTENT_STATE);
                for (DLedgerEntry entry : request.getBatchEntry()) {
                    dLedgerStore.appendAsFollower(entry, request.getTerm(), request.getLeaderId());
                }
                future.complete(buildResponse(request, DLedgerResponseCode.SUCCESS.getCode()));
                updateCommittedIndex(request.getTerm(), request.getCommitIndex());
            } catch (Throwable t) {
                logger.error("[HandleDoBatchAppend]", t);
                future.complete(buildResponse(request, DLedgerResponseCode.INCONSISTENT_STATE.getCode()));
            }

        }
    }
}
