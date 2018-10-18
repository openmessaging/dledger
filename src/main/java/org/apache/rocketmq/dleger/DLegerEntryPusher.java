package org.apache.rocketmq.dleger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.PushEntryRequest;
import org.apache.rocketmq.dleger.protocol.PushEntryResponse;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.apache.rocketmq.dleger.utils.Pair;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.apache.rocketmq.dleger.utils.UtilAll;
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
                    UtilAll.sleep(10);
                }
        }
    }

    private class EntryDispatcher extends ShutdownAbleThread {

        private AtomicReference<PushEntryRequest.Type> type = new AtomicReference<>(PushEntryRequest.Type.WRITE);
        private String peerId;
        private long compareIndex = dLegerStore.getLegerEndIndex();
        private long writeIndex = dLegerStore.getLegerEndIndex() + 1;
        private int maxPendingSize = 100;
        private ConcurrentMap<Long, CompletableFuture<PushEntryResponse>> pendingMap = new ConcurrentHashMap<>();

        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher-" + peerId, logger);
            this.peerId = peerId;
        }

        private void doWrite() throws Exception {
            while (true) {
                if (!memberState.isLeader()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.WRITE) {
                    break;
                }
                if (writeIndex > dLegerStore.getLegerEndIndex()) {
                    break;
                }
                if (pendingMap.size() >= maxPendingSize) {
                    break;
                }
                DLegerEntry entry = dLegerStore.get(writeIndex);
                PreConditions.check(entry != null, DLegerResponseCode.INTERNAL_ERROR, "writeIndex=%d", writeIndex);
                PushEntryRequest request = new PushEntryRequest();
                request.setRemoteId(peerId);
                request.setLeaderId(memberState.getSelfId());
                request.setTerm(memberState.currTerm());
                request.setEntry(entry);
                CompletableFuture<PushEntryResponse> responseFuture = dLegerRpcService.push(request);
                pendingMap.put(writeIndex, responseFuture);
                responseFuture.whenComplete((x, ex) -> {
                    if (x.getCode() == DLegerResponseCode.SUCCESS.getCode()) {
                        pendingMap.remove(x.getIndex());
                        updatePeerWaterMark(peerId, x.getIndex());
                        quorumAckChecker.wakeup();
                    } else if (x.getCode() == DLegerResponseCode.INCONSISTENT_STATE.getCode()) {
                        logger.info("Get INCONSISTENT_STATE when push to {} at {}", peerId, x.getIndex());
                        if(this.type.compareAndSet(PushEntryRequest.Type.WRITE, PushEntryRequest.Type.COMPARE)) {
                            compareIndex = dLegerStore.getLegerEndIndex();
                            pendingMap.clear();
                        }
                    } else {
                        //TODO should redispatch
                        logger.info("Unexpected response in entry dispatcher {} ", x);
                    }
                });
                writeIndex++;
            }
        }

        public void doTruncate(long truncateIndex) throws Exception {
            logger.info("Will push data to truncate {} at {}", peerId, truncateIndex);
            DLegerEntry truncateEntry = dLegerStore.get(truncateIndex);
            PreConditions.check(truncateEntry != null, DLegerResponseCode.UNKNOWN);
            PushEntryRequest truncateRequest = new PushEntryRequest();
            truncateRequest.setRemoteId(peerId);
            truncateRequest.setLeaderId(memberState.getSelfId());
            truncateRequest.setTerm(memberState.currTerm());
            truncateRequest.setEntry(truncateEntry);
            truncateRequest.setType(PushEntryRequest.Type.TRUNCATE);
            PushEntryResponse truncateResponse = dLegerRpcService.push(truncateRequest).get(3, TimeUnit.SECONDS);
            PreConditions.check(truncateResponse != null, DLegerResponseCode.UNKNOWN, null);
            PreConditions.check(truncateResponse.getCode() == DLegerResponseCode.SUCCESS.getCode(), DLegerResponseCode.UNKNOWN, null);
            type.set(PushEntryRequest.Type.WRITE);
            writeIndex =  truncateIndex;
        }

        public void doCompare() throws Exception {
            while (true) {
                if (!memberState.isLeader()) {
                    break;
                }
                if (type.get() != PushEntryRequest.Type.COMPARE) {
                    break;
                }
                DLegerEntry entry = dLegerStore.get(compareIndex);
                PreConditions.check(entry != null, DLegerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PushEntryRequest request = new PushEntryRequest();
                request.setRemoteId(peerId);
                request.setLeaderId(memberState.getSelfId());
                request.setTerm(memberState.currTerm());
                request.setEntry(entry);
                request.setType(PushEntryRequest.Type.COMPARE);
                CompletableFuture<PushEntryResponse> responseFuture = dLegerRpcService.push(request);
                PushEntryResponse response = responseFuture.get(3, TimeUnit.SECONDS);
                PreConditions.check(response != null, DLegerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                PreConditions.check(response.getCode() == DLegerResponseCode.INCONSISTENT_STATE.getCode(), DLegerResponseCode.INTERNAL_ERROR, "compareIndex=%d", compareIndex);
                long truncateIndex = -1;
                if (response.getCode() == DLegerResponseCode.SUCCESS.getCode()) {
                    if (compareIndex == response.getEndIndex()) {
                        writeIndex =  compareIndex + 1;
                        type.set(PushEntryRequest.Type.WRITE);
                        break;
                    } else {
                        truncateIndex = compareIndex;
                    }
                } else if (response.getEndIndex() < dLegerStore.getLegerBeginIndex()
                    || response.getBeginIndex() > dLegerStore.getLegerEndIndex()) {
                    truncateIndex = dLegerStore.getLegerBeginIndex();
                } else if(compareIndex < response.getBeginIndex()) {
                    truncateIndex = dLegerStore.getLegerBeginIndex();
                } else if (compareIndex > response.getEndIndex()){
                    compareIndex =  response.getEndIndex();
                } else {
                    compareIndex--;
                }
                if (compareIndex < dLegerStore.getLegerBeginIndex()) {
                    truncateIndex = dLegerStore.getLegerBeginIndex();
                }
                if (truncateIndex != -1) {
                    compareIndex = dLegerStore.getLegerEndIndex();
                    doTruncate(truncateIndex);
                    break;
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
                    doCompare();
                }
                waitForRunning(1);
            } catch (Throwable t) {
                DLegerEntryPusher.this.logger.error("Error in {}", getName(),  t);
                UtilAll.sleep(10);
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
            long index = request.getEntry().getIndex();
            if (request.getType() == PushEntryRequest.Type.WRITE) {
                requestMap.put(request.getEntry().getIndex(), new Pair<>(request, future));
                wakeup();
                return future;
            } else if (request.getType() == PushEntryRequest.Type.COMPARE) {
                requestMap.clear();
                return handleDoCompare(index, request, future);
            } else if (request.getType() == PushEntryRequest.Type.TRUNCATE) {
                requestMap.clear();
                return handleDoTruncate(index, request, future);
            } else {
                logger.error("Unknown type {} at {}", request.getType(), index);
                future.complete(buildResponse(request, DLegerResponseCode.ILLEGAL_ERROR.getCode()));
                return future;
            }
        }



        private PushEntryResponse buildResponse(PushEntryRequest request, int code) {
            PushEntryResponse response = new PushEntryResponse();
            response.setCode(code);
            response.setTerm(request.getTerm());
            response.setIndex(request.getEntry().getIndex());
            response.setBeginIndex(dLegerStore.getLegerBeginIndex());
            response.setEndIndex(dLegerStore.getLegerEndIndex());
            return response;
        }

        private void handleDoWrite(long writeIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(writeIndex == request.getEntry().getIndex(), DLegerResponseCode.INCONSISTENT_STATE);
                long index = dLegerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == writeIndex, DLegerResponseCode.INCONSISTENT_STATE);
                PushEntryResponse response = new PushEntryResponse();
                response.setTerm(request.getTerm());
                response.setIndex(index);
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoWrite] writeIndex={}", writeIndex, t);
                future.complete(buildResponse(request, DLegerResponseCode.INCONSISTENT_STATE.getCode()));
            }
        }

        private CompletableFuture<PushEntryResponse> handleDoCompare(long compareIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                DLegerEntry remote =  request.getEntry();
                PreConditions.check(compareIndex == remote.getIndex(), DLegerResponseCode.INCONSISTENT_STATE);
                DLegerEntry local = dLegerStore.get(compareIndex);
                PreConditions.check(remote.equals(local), DLegerResponseCode.INCONSISTENT_STATE);
                PushEntryResponse response = new PushEntryResponse();
                response.setTerm(request.getTerm());
                response.setIndex(compareIndex);
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoCompare] compareIndex={}", compareIndex, t);
                future.complete(buildResponse(request, DLegerResponseCode.INCONSISTENT_STATE.getCode()));
            }
            return future;
        }

        private CompletableFuture<PushEntryResponse> handleDoTruncate(long truncateIndex, PushEntryRequest request, CompletableFuture<PushEntryResponse> future) {
            try {
                PreConditions.check(truncateIndex == request.getEntry().getIndex(), DLegerResponseCode.INCONSISTENT_STATE);
                long index = dLegerStore.truncate(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == truncateIndex, DLegerResponseCode.INCONSISTENT_STATE);
                PushEntryResponse response = new PushEntryResponse();
                response.setTerm(request.getTerm());
                response.setIndex(index);
                future.complete(response);
            } catch (Throwable t) {
                logger.error("[HandleDoTruncate] truncateIndex={}", truncateIndex, t);
                future.complete(buildResponse(request, DLegerResponseCode.INCONSISTENT_STATE.getCode()));
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
                UtilAll.sleep(10);
            }
        }
    }
}
