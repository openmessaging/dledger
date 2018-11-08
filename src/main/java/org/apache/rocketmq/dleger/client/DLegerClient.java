package org.apache.rocketmq.dleger.client;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.dleger.ShutdownAbleThread;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.GetEntriesRequest;
import org.apache.rocketmq.dleger.protocol.GetEntriesResponse;
import org.apache.rocketmq.dleger.protocol.MetadataRequest;
import org.apache.rocketmq.dleger.protocol.MetadataResponse;
import org.apache.rocketmq.dleger.utils.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerClient {

    private static Logger logger = LoggerFactory.getLogger(DLegerClient.class);
    private final Map<String, String> peerMap = new ConcurrentHashMap<>();
    private String leaderId;
    private final String group;
    private DLegerClientRpcService dLegerClientRpcService;

    private MetadataUpdater metadataUpdater = new MetadataUpdater("MetadataUpdater", logger);


    public DLegerClient(String group, String peers) {
        this.group = group;
        updatePeers(peers);
        dLegerClientRpcService = new DLegerClientRpcNettyService();
        dLegerClientRpcService.updatePeers(peers);
        leaderId = peerMap.keySet().iterator().next();
    }


    public AppendEntryResponse append(byte[] body) {
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setCode(DLegerResponseCode.METADATA_ERROR.getCode());
                return appendEntryResponse;
            }
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setGroup(group);
            appendEntryRequest.setRemoteId(leaderId);
            appendEntryRequest.setBody(body);
            AppendEntryResponse response = dLegerClientRpcService.append(appendEntryRequest).get();
            if (response.getCode() == DLegerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    appendEntryRequest.setRemoteId(leaderId);
                    response = dLegerClientRpcService.append(appendEntryRequest).get();
                }
            }
            return response;
        } catch (Exception e) {
            logger.error("{}", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLegerResponseCode.INTERNAL_ERROR.getCode());
            return appendEntryResponse;
        }
    }

    public GetEntriesResponse get(long index){
        try {
            waitOnUpdatingMetadata(1500, false);
            if (leaderId == null) {
                GetEntriesResponse response = new GetEntriesResponse();
                response.setCode(DLegerResponseCode.METADATA_ERROR.getCode());
                return response;
            }

            GetEntriesRequest request = new GetEntriesRequest();
            request.setGroup(group);
            request.setRemoteId(leaderId);
            request.setBeginIndex(index);
            GetEntriesResponse response = dLegerClientRpcService.get(request).get();
            if (response.getCode() == DLegerResponseCode.NOT_LEADER.getCode()) {
                waitOnUpdatingMetadata(1500, true);
                if (leaderId != null) {
                    request.setRemoteId(leaderId);
                    response = dLegerClientRpcService.get(request).get();
                }
            }
            return response;
        } catch (Exception e) {
            GetEntriesResponse getEntriesResponse =  new GetEntriesResponse();
            getEntriesResponse.setCode(DLegerResponseCode.INTERNAL_ERROR.getCode());
            return getEntriesResponse;
        }
    }



    public void startup() {
        this.dLegerClientRpcService.startup();
        this.metadataUpdater.start();
    }

    public void shutdown() {
        this.dLegerClientRpcService.shutdown();
        this.metadataUpdater.shutdown();
    }


    private void updatePeers(String peers) {
        for (String peerInfo: peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }


    private synchronized void waitOnUpdatingMetadata(long maxWaitMs, boolean needFresh) {
        if (needFresh) {
            leaderId = null;
        } else if (leaderId != null) {
            return;
        }
        long start = System.currentTimeMillis();
        while (UtilAll.elapsed(start) < maxWaitMs && leaderId == null) {
            metadataUpdater.wakeup();
            try {
                wait(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private class MetadataUpdater extends ShutdownAbleThread {

        public MetadataUpdater(String name, Logger logger) {
            super(name, logger);
        }


        private void getMedata(String peerId) {
            try {
                MetadataRequest request = new MetadataRequest();
                request.setGroup(group);
                request.setRemoteId(peerId);
                CompletableFuture<MetadataResponse> future = dLegerClientRpcService.metadata(request);
                MetadataResponse response = future.get(1500, TimeUnit.MILLISECONDS);
                if (response.getLeaderId() != null) {
                    leaderId = response.getLeaderId();
                    if (response.getPeers() != null) {
                        peerMap.putAll(response.getPeers());
                        dLegerClientRpcService.updatePeers(response.getPeers());
                    }
                }
            } catch (Throwable t) {
                logger.error("Get metadata failed from {}", peerId, t);
            }

        }
        @Override public void doWork() {
            try {
                if (leaderId == null) {
                    for (String peer: peerMap.keySet()) {
                        getMedata(peer);
                        if (leaderId != null) {
                            synchronized (DLegerClient.this) {
                                DLegerClient.this.notifyAll();
                            }
                            break;
                        }
                    }
                } else {
                    getMedata(leaderId);
                }
                waitForRunning(3000);
            } catch (Throwable t) {
                logger.error("Error", t);
                UtilAll.sleep(1000);
            }
        }
    }


}
