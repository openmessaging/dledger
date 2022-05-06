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

package io.openmessaging.storage.dledger.dledger;

import io.openmessaging.storage.dledger.DLedgerRpcNettyService;
import io.openmessaging.storage.dledger.DLedgerRpcService;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;


public class DLedgerProxy implements DLedgerProtocolHandler {

    private static Logger logger = LoggerFactory.getLogger(DLedgerProxy.class);

    private DLedgerManager dLedgerManager;

    private ConfigManager configManager;

    private DLedgerRpcService dLedgerRpcService;

    public DLedgerProxy(DLedgerProxyConfig dLedgerProxyConfig){
        this.configManager = new ConfigManager(dLedgerProxyConfig);
        this.dLedgerRpcService = new DLedgerRpcNettyService(this);
        this.dLedgerManager = new DLedgerManager(dLedgerProxyConfig, this.dLedgerRpcService);
    }

    public DLedgerManager getDLedgerManager() {
        return dLedgerManager;
    }

    public void setDLedgerManager(DLedgerManager dLedgerManager) {
        this.dLedgerManager = dLedgerManager;
    }

    public ConfigManager getConfigManager() {
        return configManager;
    }

    public void setConfigManager(ConfigManager configManager) {
        this.configManager = configManager;
    }

    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleAppend(request);
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleGet(request);
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleMetadata(request);
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(LeadershipTransferRequest leadershipTransferRequest) throws Exception {
        return this.dLedgerManager.getDLedgerServer(leadershipTransferRequest.getGroup(), leadershipTransferRequest.getRemoteId()).handleLeadershipTransfer(leadershipTransferRequest);
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleVote(request);
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleHeartBeat(request);
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handlePull(request);
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handlePush(request);
    }

    public void startup() {
        this.dLedgerRpcService.startup();
        this.dLedgerManager.startup();
    }

    public DLedgerManager getdLedgerManager() {
        return dLedgerManager;
    }

    public void setdLedgerManager(DLedgerManager dLedgerManager) {
        this.dLedgerManager = dLedgerManager;
    }

    public DLedgerRpcService getdLedgerRpcService() {
        return dLedgerRpcService;
    }

    public void setdLedgerRpcService(DLedgerRpcService dLedgerRpcService) {
        this.dLedgerRpcService = dLedgerRpcService;
    }

    public void shutdown() {
        this.dLedgerManager.shutdown();
    }

}
