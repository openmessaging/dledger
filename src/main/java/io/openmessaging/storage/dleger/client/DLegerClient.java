/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dleger.client;

import io.openmessaging.storage.dleger.ShutdownAbleThread;
import io.openmessaging.storage.dleger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;
import io.openmessaging.storage.dleger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dleger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dleger.protocol.MetadataRequest;
import io.openmessaging.storage.dleger.protocol.MetadataResponse;
import io.openmessaging.storage.dleger.utils.UtilAll;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerClient {

    private static Logger logger = LoggerFactory.getLogger(DLegerClient.class);
    private final Map<String, String> peerMap = new ConcurrentHashMap<>();
    private final String group;
    private String leaderId;
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
            needFreshMetadata();
            logger.error("{}", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLegerResponseCode.INTERNAL_ERROR.getCode());
            return appendEntryResponse;
        }
    }

    public GetEntriesResponse get(long index) {
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
        } catch (Exception t) {
            needFreshMetadata();
            logger.error("", t);
            GetEntriesResponse getEntriesResponse = new GetEntriesResponse();
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
        for (String peerInfo : peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }

    private synchronized void needFreshMetadata() {
        leaderId = null;
        metadataUpdater.wakeup();
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

        private void getMetadata(String peerId, boolean isLeader) {
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
                if (isLeader) {
                    needFreshMetadata();
                }
                logger.warn("Get metadata failed from {}", peerId, t);
            }

        }

        @Override public void doWork() {
            try {
                if (leaderId == null) {
                    for (String peer : peerMap.keySet()) {
                        getMetadata(peer, false);
                        if (leaderId != null) {
                            synchronized (DLegerClient.this) {
                                DLegerClient.this.notifyAll();
                            }
                            UtilAll.sleep(1000);
                            break;
                        }
                    }
                } else {
                    getMetadata(leaderId, true);
                }
                waitForRunning(3000);
            } catch (Throwable t) {
                logger.error("Error", t);
                UtilAll.sleep(1000);
            }
        }
    }

}
