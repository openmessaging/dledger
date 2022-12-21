///*
// * Copyright 2017-2022 The DLedger Authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      https://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//
//package io.openmessaging.storage.dledger;
///*
//    create qiangzhiwei time 2022/11/26
// */
//
//import io.openmessaging.storage.dledger.entry.DLedgerEntry;
//import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
//import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
//import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
//import io.openmessaging.storage.dledger.store.DLedgerStore;
//import io.openmessaging.storage.dledger.utils.DLedgerUtils;
//import io.openmessaging.storage.dledger.utils.Pair;
//import io.openmessaging.storage.dledger.utils.PreConditions;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//
//public class DLedgerEntryPusherToLearner extends AbstractDLedgerEntryPusher {
//    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerEntryPusher.class);
//
//    private final Map<String, EntryDispatcher> learnerDispatcherMap = new HashMap<>();
//
//    public DLedgerEntryPusherToLearner(DLedgerConfig dLedgerConfig, MemberState memberState, DLedgerStore dLedgerStore,
//                                       DLedgerRpcService dLedgerRpcService) {
//        super(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);
//        for (String learner: memberState.getLearnerMap().keySet()) {
//            learnerDispatcherMap.put(learner, new EntryDispatcher(learner, LOGGER));
//        }
//    }
//
//    @Override
//    public long getPeerWaterMark(long term, String peerId) {
//        return 0;
//    }
//
//
//    class EntryDispatcher extends AbstractEntryDispatcher {
//
//        private long committedIndex = -1;
//
//        protected long lastCheckLeakTimeMs = System.currentTimeMillis();
//
//        protected final ConcurrentMap<Long, Long> learnerPendingMap = new ConcurrentHashMap<>();
//        protected final ConcurrentMap<Long, Pair<Long, Integer>> learnerBatchPendingMap = new ConcurrentHashMap<>();
//
//
//        public EntryDispatcher(String peerId, Logger logger) {
//            super(peerId, logger);
//        }
//
//        @Override
//        protected void changeState(long index, PushEntryRequest.Type target) {
//            logger.info("[Push-{}]Learner Change state from {} to {} at {}", peerId, type.get(), target, index);
//            switch (target) {
//                case APPEND:
//                    compareIndex = -1;
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
//                    compareIndex = -1;
//                    break;
//                default:
//                    break;
//            }
//            type.set(target);
//        }
//
//        protected void doCommit() throws Exception {
//            if (DLedgerUtils.elapsed(lastPushCommitTimeMs) > 1000) {
//                PushEntryRequest request = buildPushRequest(null, PushEntryRequest.Type.COMMIT);
//                request.setCommitIndex(committedIndex);
//                //Ignore the results
//                dLedgerRpcService.push(request);
//                lastPushCommitTimeMs = System.currentTimeMillis();
//            }
//        }
//
//        public void updateCommitIndex(long index) {
//            if (index > this.committedIndex) {
//                this.committedIndex = index;
//            } else {
//                logger.warn("index: [{}] update error, committedIndex: [{}]", index, committedIndex);
//            }
//        }
//
//        protected void sendBatchAppendEntryRequest() throws Exception {
//            batchAppendEntryRequest.setCommitIndex(committedIndex);
//            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(batchAppendEntryRequest);
//            batchPendingMap.put(batchAppendEntryRequest.getFirstEntryIndex(), new Pair<>(System.currentTimeMillis(), batchAppendEntryRequest.getCount()));
//            responseFuture.whenComplete((x, ex) -> {
//                try {
//                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
//                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
//                    switch (responseCode) {
//                        case SUCCESS:
//                            batchPendingMap.remove(x.getIndex());
//                            updateCommitIndex(x.getIndex() + x.getCount() - 1);
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
//        }
//
//
//        protected void doAppendInner(long index) throws Exception {
//            DLedgerEntry entry = getDLedgerEntryForAppend(index);
//            if (null == entry) {
//                return;
//            }
//            checkQuotaAndWait(entry);
//            PushEntryRequest request = buildPushRequest(entry, PushEntryRequest.Type.APPEND);
//            CompletableFuture<PushEntryResponse> responseFuture = dLedgerRpcService.push(request);
//            pendingMap.put(index, System.currentTimeMillis());
//            responseFuture.whenComplete((x, ex) -> {
//                try {
//                    PreConditions.check(ex == null, DLedgerResponseCode.UNKNOWN);
//                    DLedgerResponseCode responseCode = DLedgerResponseCode.valueOf(x.getCode());
//                    switch (responseCode) {
//                        case SUCCESS:
//                            pendingMap.remove(index);
//                            updateCommitIndex(x.getIndex());
//                            break;
//                        case INCONSISTENT_STATE:
//                            logger.info("[Push-{}]Get INCONSISTENT_STATE when push index={} term={}", peerId, x.getIndex(), x.getTerm());
//                            changeState(-1, PushEntryRequest.Type.COMPARE);
//                        default:
//                            logger.warn("[Push-{}]Get error response code {} {}", peerId, responseCode, x.baseInfo());
//                    }
//                } catch (Throwable t) {
//                    logger.error("append error, index: {}", index, t);
//                }
//            });
//            lastPushCommitTimeMs = System.currentTimeMillis();
//        }
//    }
//
//    @Override
//    public void startup() {
//        for (EntryDispatcher dispatcher : learnerDispatcherMap.values()) {
//            dispatcher.start();
//        }
//    }
//
//
//    public void shutdown() {
//        for (EntryDispatcher dispatcher : learnerDispatcherMap.values()) {
//            dispatcher.shutdown();
//        }
//    }
//}
