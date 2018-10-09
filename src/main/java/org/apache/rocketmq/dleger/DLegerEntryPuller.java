package org.apache.rocketmq.dleger;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.PullEntriesRequest;
import org.apache.rocketmq.dleger.protocol.PullEntriesResponse;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerEntryPuller {

    private Logger logger = LoggerFactory.getLogger(DLegerEntryPuller.class);

    private Map<Long, CompletableFuture<AppendEntryResponse>> pendingAppendEntryResponseMap = new ConcurrentHashMap<>();
    private AtomicLong nextIndex = new AtomicLong();

    private DLegerStore dLegerStore;

    private DLegerConfig dLegerConfig;

    private MemberState memberState;

    private DLegerRpcService dLegerRpcService;

    private Map<String, Long> peerWaterMarks = new ConcurrentHashMap<>();

    private Worker worker = new Worker("DLegerStorePuller", logger);

    public DLegerEntryPuller(DLegerConfig dLegerConfig, MemberState memberState, DLegerStore dLegerStore, DLegerRpcService dLegerRpcService) {
        this.dLegerConfig = dLegerConfig;
        this.memberState =  memberState;
        this.dLegerStore = dLegerStore;
        this.dLegerRpcService = dLegerRpcService;
    }

    public void startup() {
        worker.start();
    }

    public void shutdown() {
        worker.shutdown();
    }


    public void checkQuorumIndex(Long index) {
        if (memberState.checkQuorumIndex(index)) {
            for (Long i = index; i >= 0 ; i--) {
                CompletableFuture<AppendEntryResponse> future = pendingAppendEntryResponseMap.remove(i);
                if (future == null) {
                    break;
                } else {
                    AppendEntryResponse response = new AppendEntryResponse();
                    response.setIndex(i);
                    future.complete(response);
                }
            }
        }
    }

    CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) {
        DLegerEntry dLegerEntry = dLegerStore.get(request.getBeginIndex());
        PullEntriesResponse response = new PullEntriesResponse();
        if (dLegerEntry != null) {
            memberState.updateIndex(request.getNodeId(), dLegerEntry.getIndex());
            //TODO should check by confirmed index
            checkQuorumIndex(dLegerEntry.getIndex());
            response.getEntries().add(dLegerEntry);
        }
        return CompletableFuture.completedFuture(response);
    }


    private class Worker extends ShutdownAbleThread {

        public Worker(String name, Logger logger) {
            super(name, logger);
        }

        @Override
        public void doWork() {
                try {
                    if (dLegerConfig.isEnablePushToFollower()) {
                        Thread.sleep(100);
                        return;
                    }
                    //prev term need to lock
                    if (!memberState.isFollower()) {
                        Thread.sleep(100);
                        return;
                    }
                    //TODO polish the performance
                    synchronized (memberState) {
                        if (memberState.isFollower()) {
                            PullEntriesRequest request = new PullEntriesRequest();
                            request.setNodeId(memberState.getSelfId());
                            request.setBeginIndex(nextIndex.get());
                            PullEntriesResponse response = dLegerRpcService.pull(request).get();
                            if (response.getEntries() != null && response.getEntries().size() > 0) {
                                long maxIndex = -1;
                                for (DLegerEntry entry: response.getEntries()) {
                                    if (entry.getIndex() > maxIndex) {
                                        maxIndex = entry.getIndex();
                                    }
                                    dLegerStore.appendAsFollower(entry, memberState.currTerm(), memberState.getLeaderId());
                                }
                                nextIndex.set(maxIndex + 1);
                            } else {
                                Thread.sleep(1);
                            }
                        }
                    }

                } catch (Throwable t) {
                    DLegerEntryPuller.this.logger.error("Error in puller", t);
                }
        }
    }
}
