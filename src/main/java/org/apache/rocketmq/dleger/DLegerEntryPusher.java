package org.apache.rocketmq.dleger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
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
                        Thread.sleep(1);
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
                        Thread.yield();
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

        private String peerId;
        private long currIndex  = -1;
        private int maxPendingSize = 100;
        private ConcurrentMap<Long, CompletableFuture<PushEntryResponse>> pendingMap = new ConcurrentHashMap<>();
        public EntryDispatcher(String peerId, Logger logger) {
            super("EntryDispatcher", logger);
            this.peerId = peerId;
        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isLeader()) {
                    Thread.sleep(1);
                    return;
                }
                while (true) {
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
                    reponseFuture.thenAccept(x -> {
                       pendingMap.remove(x.getIndex());
                       updatePeerWaterMark(peerId, x.getIndex());
                    });
                }
                Thread.sleep(1);
            } catch (Throwable t) {
                DLegerEntryPusher.this.logger.error("Error in {}", getName(),  t);
            }
        }
    }

    private class EntryHandler extends ShutdownAbleThread {

        ConcurrentMap<Long, Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>>> requestMap = new ConcurrentHashMap<>();

        public CompletableFuture<PushEntryResponse>  handlePush(PushEntryRequest request) {
            CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
            requestMap.put(request.getEntry().getIndex(), new Pair<>(request, future));
            return future;
        }

        public EntryHandler(Logger logger) {
            super("EntryHandler", logger);
        }

        @Override
        public void doWork() {
            try {
                if (!memberState.isFollower()) {
                    Thread.sleep(1);
                    return;
                }
                long legerEndIndex = dLegerStore.getLegerEndIndex();

                Pair<PushEntryRequest, CompletableFuture<PushEntryResponse>> pair  = requestMap.remove(++legerEndIndex);
                if (pair == null) {
                    Thread.sleep(1);
                    return;
                }
                PushEntryRequest request = pair.getKey();
                long index = dLegerStore.appendAsFollower(request.getEntry(), request.getTerm(), request.getLeaderId());
                PreConditions.check(index == request.getEntry().getIndex(), DLegerException.Code.UNCONSISTENCT_STATE, null);
                PushEntryResponse response = new PushEntryResponse();
                response.setTerm(request.getTerm());
                response.setIndex(index);
                pair.getValue().complete(response);
            } catch (Throwable t) {
                DLegerEntryPusher.this.logger.error("Error in {}", getName(),  t);
            }
        }
    }
}
