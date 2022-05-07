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

import io.openmessaging.storage.dledger.AppendFuture;
import io.openmessaging.storage.dledger.DLedgerRpcNettyService;
import io.openmessaging.storage.dledger.DLedgerRpcService;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerProtocolHandler;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.MetadataRequest;
import io.openmessaging.storage.dledger.protocol.MetadataResponse;
import io.openmessaging.storage.dledger.protocol.PullEntriesRequest;
import io.openmessaging.storage.dledger.protocol.PullEntriesResponse;
import io.openmessaging.storage.dledger.protocol.PushEntryRequest;
import io.openmessaging.storage.dledger.protocol.PushEntryResponse;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * DLedgerServers' proxy, which can handle the request and route it to different DLedgerServer
 */
public class DLedgerProxy implements DLedgerProtocolHandler {

    private static Logger logger = LoggerFactory.getLogger(DLedgerProxy.class);

    private DLedgerManager dLedgerManager;

    private ConfigManager configManager;

    private DLedgerRpcService dLedgerRpcService;

    public DLedgerProxy(final DLedgerProxyConfig dLedgerProxyConfig) {
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
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleAppend(request);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandleAppend] failed", dLedgerException);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleGet(request);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandleGet] failed", dLedgerException);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleMetadata(request);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandleMetaData] failed", dLedgerException);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(LeadershipTransferRequest leadershipTransferRequest) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(leadershipTransferRequest.getGroup(), leadershipTransferRequest.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", leadershipTransferRequest.getGroup(), leadershipTransferRequest.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(leadershipTransferRequest.getGroup(), leadershipTransferRequest.getRemoteId()).handleLeadershipTransfer(leadershipTransferRequest);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandleLeadershipTransfer] failed", dLedgerException);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(leadershipTransferRequest);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleVote(request);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandleVote] failed", dLedgerException);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handleHeartBeat(request);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandleHeartBeat] failed", dLedgerException);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handlePull(request);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandlePull] failed", dLedgerException);
            PullEntriesResponse response = new PullEntriesResponse();
            response.copyBaseInfo(request);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handlePush(request);
        } catch (DLedgerException dLedgerException) {
            logger.error("[Proxy][HandlePush] failed", dLedgerException);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(dLedgerException.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
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
