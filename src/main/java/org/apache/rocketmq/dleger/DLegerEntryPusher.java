package org.apache.rocketmq.dleger;

import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.JarEntry;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.PushEntryRequest;
import org.apache.rocketmq.dleger.protocol.PushEntryResponse;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.apache.rocketmq.dleger.utils.Pair;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerEntryPusher {

    private Logger logger = LoggerFactory.getLogger(DLegerEntryPusher.class);

    private DLegerConfig dLegerConfig;
    private DLegerStore dLegerStore;

    private MemberState memberState;

    private DLegerRpcService dLegerRpcService;

    private Map<String, Long> peerWaterMarks = new ConcurrentHashMap<>();
    private Map<Long, CompletableFuture<AppendEntryResponse>> pendingAppendEntryResponseMap = new ConcurrentHashMap<>();


    private EntryHandler entryHandler = new EntryHandler(logger);


    private QuorumAckChecker quorumAckChecker = new QuorumAckChecker(logger);


    private Map<String, EntryDispatcher> dispatcherMap = new HashMap<>();

    public DLegerEntryPusher(DLegerConfig dLegerConfig, MemberState memberState, DLegerStore dLegerStore, DLegerRpcService dLegerRpcService) {
        this.dLegerConfig = dLegerConfig;
        this.memberState =  memberState;
        this.dLegerStore = dLegerStore;
        this.dLegerRpcService = dLegerRpcService;
        for (String peer: memberState.getPeerMap().keySet()) {
            if (!peer.equals(memberState.getSelfId())) {
                dispatcherMap.put(peer, new EntryDispatcher(peer, logger));
            }
        }
    }


    public void startup() {
        entryHandler.start();
        quorumAckChecker.start();
        for (String peerId: memberState.getPeerMap().keySet()) {
            peerWaterMarks.put(peerId, -1L);
        }
        for (EntryDispatcher dispatcher: dispatcherMap.values()) {
            dispatcher.start();
        }
    }

    public void shutdown() {
        entryHandler.shutdown();
        quorumAckChecker.shutdown();
        for (EntryDispatcher dispatcher: dispatcherMap.values()) {
            dispatcher.shutdown();
        }
    }

    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return entryHandler.handlePush(request);
    }


    private void updatePeerWaterMark(String peerId, long index) {
        synchronized (peerWaterMarks) {
            if (peerWaterMarks.get(peerId) < index) {
                peerWaterMarks.put(peerId, index);
            }
        }
    }

    public void waitAck(Long index, CompletableFuture<AppendEntryResponse> future) {
        updatePeerWaterMark(memberState.getLeaderId(), index);
        if (memberState.getPeerMap().size() == 1) {
            AppendEntryResponse response = new AppendEntryResponse();
            response.setIndex(index);
            future.complete(response);
        }  else {
            pendingAppendEntryResponseMap.put(index, future);
            wakeUpDispatchers();
        }
    }

    public void wakeUpDispatchers() {
        for (EntryDispatcher dispatcher: dispatcherMap.values()) {
            dispatcher.wakeup();
        }
    }


    private class QuorumAckChecker extends ShutdownAbleThread {

        public QuorumAckChecker(Logger logger) {
            super("QuorumAckChecker", logger);
        }

        @Override
        public void doWork() {
                try {
                    if (!memberState.isLeader()) {
                        waitForRunning(1);
                        return;
                    }
                    long quorumIndex = -1;
                    for (Long index: peerWaterMarks.values()) {
                        int num = 0;
                        for (Long another: peerWaterMarks.values()) {
                            if (another >= index) {
                                num++;
                            }
                        }
                        if (memberState.isQuorum(num)) {
                            quorumIndex = index;
                            break;
                        }
                    }
                    if (quorumIndex == -1) {
                        waitForRunning(1);
                        return;
                    }
                    for (Long i = quorumIndex; i >= 0 ; i--) {
                        CompletableFuture<AppendEntryResponse> future = pendingAppendEntryResponseMap.remove(i);
                        if (future != null) {
                            AppendEntryResponse response = new AppendEntryResponse();
                            response.setIndex(i);
                            future.complete(response);
                        } else {
                            break;
                        }
                    }
                } catch (Throwable t) {
                    DLegerEntryPusher.this.logger.error("Error in {}", getName(), t);
                }
        }
    }

    private class EntryDispatcher extends ShutdownAbleThread {

        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.WRITE);
        private String peerId;
        private long currIndex  = dLegerStore.getLegerEndIndex();
        private int maxPendingSize = 100;
        private ConcurrentMap<Long, CompletableFuture<PushEntryResponse>> pendingMap = new ConcurrentHashMap<>();

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + peerId, logger);
            this.peerId = peerId;
        }

        public void doWrite() throws Exception {
            while (true) {
                if (type.get() != PushEntryRequest.Type.WRITE) {
                    break;
                }
                if (currIndex >= dLegerStore.getLegerEndIndex()) {
                    break;
                }
                if (pendingMap.size() >= maxPendingSize) {
                    break;
                }
                DLegerEntry entry = dLegerStore.get(currIndex + 1);
                if (entry == null) {
                    break;
                }
                currIndex++;
                PushEntryRequest request = new PushEntryRequest();
                request.setRemoteId(peerId);
                request.setLeaderId(memberState.getSelfId());
                request.setTerm(memberState.currTerm());
                request.setEntry(entry);
                CompletableFuture<PushEntryResponse> reponseFuture = dLegerRpcService.push(request);
                pendingMap.put(currIndex, reponseFuture);
                reponseFuture.whenComplete((x, ex) -> {
                    if (x.getCode() == DLegerResponseCode.SUCCESS.getCode()) {
                        pendingMap.remove(x.getIndex());
                        updatePeerWaterMark(peerId, x.getIndex());
                        quorumAckChecker.wakeup();
                    } else if (x.getCode() == DLegerResponseCode.UNCONSISTENCT_STATE.getCode()) {
                        logger.info("Get UNCONSISTENCT_STATE when push to {} at {}", peerId, x.getIndex());
                        this.type.compareAndSet(PushEntryRequest.Type.WRITE, PushEntryRequest.Type.COMPARE);
                    } else {
                        //TODO
                        logger.info("Some error in entry dispatcher");
                    }
                });
            }
        }

        public void doTruncate(long truncateIndex) {
            try {
                currIndex =  truncateIndex;
                logger.info("Trying to truncate {} at {}", peerId, truncateIndex);
                DLegerEntry truncateEntry = dLegerStore.get(truncateIndex);
                if (truncateEntry == null) {
                    logger.warn("This should not happen for {}", truncateIndex);
                } else {
                    PushEntryRequest truncateRequest = new PushEntryRequest();
                    truncateRequest.setRemoteId(peerId);
                    truncateRequest.setLeaderId(memberState.getSelfId());
                    truncateRequest.setTerm(memberState.currTerm());
                    truncateRequest.setEntry(truncateEntry);
                    truncateRequest.setType(PushEntryRequest.Type.TRUNCATE);
                    PushEntryResponse truncateResponse = dLegerRpcService.push(truncateRequest).get(1, TimeUnit.SECONDS);
                    PreConditions.check(truncateResponse != null, DLegerResponseCode.UNKNOWN, null);
                    PreConditions.check(truncateResponse.getCode() == DLegerResponseCode.SUCCESS.getCode(), DLegerResponseCode.UNKNOWN, null);
                }
            } catch (Exception e) {
                logger.error("Unexpected error", e);
            }

        }

        public void doTryCompareAndTruncate() {
            pendingMap.clear();
            long compareIndex = dLegerStore.getLegerEndIndex();
            logger.info("DoTryCompareAndTruncate for {} from {}", peerId, compareIndex);
            while (true) {
                try {
                    DLegerEntry entry = dLegerStore.get(compareIndex);
                    if (entry == null) {
                        logger.info("This should not happen for {}", compareIndex);
                        Thread.sleep(10);
                        continue;
                    }
                    PushEntryRequest request = new PushEntryRequest();
                    request.setRemoteId(peerId);
                    request.setLeaderId(memberState.getSelfId());
                    request.setTerm(memberState.currTerm());
                    request.setEntry(entry);
                    request.setType(PushEntryRequest.Type.COMPARE);
                    CompletableFuture<PushEntryResponse> reponseFuture = dLegerRpcService.push(request);
                    PushEntryResponse reponse = reponseFuture.get(3, TimeUnit.SECONDS);
                    if (reponse == null || reponse.getCode() != DLegerResponseCode.UNCONSISTENCT_STATE.getCode()) {
                        Thread.sleep(1);
                        continue;
                    }
                    long truncateIndex = -1;
                    if (reponse.getCode() == DLegerResponseCode.SUCCESS.getCode()) {
                        truncateIndex = compareIndex + 1;
                    } else if (reponse.getEndIndex() < dLegerStore.getLegerBeginIndex()
                        || reponse.getBeginIndex() > dLegerStore.getLegerEndIndex()) {
                        truncateIndex = dLegerStore.getLegerBeginIndex();
                    } else {
                        compareIndex--;
                    }
                    if (compareIndex == 0) {
                        truncateIndex = 0;
                    }
                    if (truncateIndex != -1) {
                        doTruncate(truncateIndex);
                        type.set(PushEntryRequest.Type.WRITE);
                        return;
                    } else {
                        compareIndex--;
                    }
                } catch (Exception e) {
                    logger.info("Some error", e);
                    try {
                        Thread.sleep(10);
                    } catch (Throwable ignored) {

                    }
                }
            }

        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isLeader()) {
                    waitForRunning(1);
                    return;
                }
                if (type.get() == PushEntryRequest.Type.WRITE) {
                    doWrite();
                } else {
                    doTryCompareAndTruncate();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLegerEntryPusher.this.logger.error("Error in {}", getName(),  t);
            }
        }
    }

    private class EntryHandler extends ShutdownAbleThread {


        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> requestMap = new ConcurrentHashMap<>();

        public EntryHandler(Logger logger) {
            super("EntryHandler", logger);
        }

        public CompletableFuture<PushEntryResponse>  handlePush(PushEntryRequest request) {
            CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
            long nextIndex = request.getEntry().getIndex();
            if (request.getType() == PushEntryRequest.Type.WRITE) {
                requestMap.put(request.getEntry().getIndex(), new Pair<>(request, future));
                wakeup();
                return future;
            } else if (request.getType() == PushEntryRequest.Type.COMPARE) {
                requestMap.clear();
                return handleDoComapre(nextIndex, request, future);
            } else if (request.getType() == PushEntryRequest.Type.TRUNCATE) {
                requestMap.clear();
                return handleDoTruncate(nextIndex, request, future);
            } else {
                logger.error("Unknown type {} at {}", request.getType(), nextIndex);
                PushEntryResponse response = new PushEntryResponse();
                response.setCode(DLegerResponseCode.INTERNAL_ERROR.getCode());
                response.setTerm(request.getTerm());
                response.setIndex(-1L);
                future.complete(response);
                return future;
            }
        }



        public PushEntryResponse buildUnconsistentResponse(PushEntryRequest request) {
            PushEntryResponse response = new PushEntryResponse();
            response.setCode(DLegerResponseCode.UNCONSISTENCT_STATE.getCode());
            response.setTerm(request.getTerm());
            response.setIndex(request.getEntry().getIndex());
            response.setBeginIndex(dLegerStore.getLegerBeginIndex());
            response.setEndIndex(dLegerStore.getLegerEndIndex());
            return response;
        }

        public void handleDoWrite(long nextIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(nextIndex == request.getEntry().getIndex(), DLegerResponseCode.UNCONSISTENCT_STATE, "Should not happen");
                long index = dLegerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == nextIndex, DLegerResponseCode.UNCONSISTENCT_STATE, "Should not happen");
                PushEntryResponse response = new PushEntryResponse();
                response.setTerm(request.getTerm());
                response.setIndex(index);
                future.complete(response);
            } catch (Exception e) {
                logger.error("[{}][HandleDoWrite] {}", memberState.getSelfId(), nextIndex, e);
                future.complete(buildUnconsistentResponse(request));
            }
        }

        public CompletableFuture<PushEntryResponse> handleDoComapre(long nextIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                DLegerEntry remote =  request.getEntry();
                PreConditions.check(nextIndex == remote.getIndex(), DLegerResponseCode.UNCONSISTENCT_STATE, "Should not happen");
                DLegerEntry local = dLegerStore.get(nextIndex);
                PreConditions.check(remote.equals(local), DLegerResponseCode.UNCONSISTENCT_STATE, null);
                PushEntryResponse response = new PushEntryResponse();
                response.setTerm(request.getTerm());
                response.setIndex(nextIndex);
                future.complete(response);
            } catch (Exception e) {
                future.complete(buildUnconsistentResponse(request));
            }
            return future;
        }

        public CompletableFuture<PushEntryResponse> handleDoTruncate(long nextIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(nextIndex == request.getEntry().getIndex(), DLegerResponseCode.UNCONSISTENCT_STATE, "Should not happen");
                long index = dLegerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == nextIndex, DLegerResponseCode.UNCONSISTENCT_STATE, "Should not happen");
                PushEntryResponse response = new PushEntryResponse();
                response.setTerm(request.getTerm());
                response.setIndex(index);
                future.complete(response);
            } catch (Exception e) {
                future.complete(buildUnconsistentResponse(request));
            }
            return future;
        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isFollower()) {
                    waitForRunning(1);
                    return;
                }
                long nextIndex = dLegerStore.getLegerEndIndex() + 1;
                Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair  = requestMap.remove(nextIndex);
                if (pair == null) {
                    waitForRunning(1);
                    return;
                }
                PushEntryRequest request = pair.getKey();
                handleDoWrite(nextIndex, request, pair.getValue());
            } catch (Throwable t) {
                DLegerEntryPusher.this.logger.error("Error in {}", getName(),  t);
            }
        }
    }
}
