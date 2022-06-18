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
import io.openmessaging.storage.dledger.DLedgerConfig;
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
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
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

    public DLedgerProxy(DLedgerProxyConfig dLedgerProxyConfig) {
        this(dLedgerProxyConfig, null, null, null);
    }

    public DLedgerProxy(DLedgerProxyConfig dLedgerProxyConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        this(dLedgerProxyConfig, nettyServerConfig, nettyClientConfig, null);
    }


    public DLedgerProxy(DLedgerProxyConfig dLedgerProxyConfig, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig, ChannelEventListener channelEventListener) {
        try {
            this.configManager = new ConfigManager(dLedgerProxyConfig);
            this.dLedgerRpcService = new DLedgerRpcNettyService(this, nettyServerConfig, nettyClientConfig, channelEventListener);
            this.dLedgerManager = new DLedgerManager(dLedgerProxyConfig, this.dLedgerRpcService);
        } catch (Exception e) {
            logger.error("[Proxy][DLedgerProxy] fail to construct", e);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * init a DLedgerServer in this DLedgerProxy
     *
     * @param dLedgerConfig
     * @param start
     * @return DLedgerServer created by the dLedgerConfig
     */
    public synchronized DLedgerServer addDLedgerServer(DLedgerConfig dLedgerConfig, boolean start) {
        this.configManager.addDLedgerConfig(dLedgerConfig);
        DLedgerServer dLedgerServer = this.dLedgerManager.addDLedgerServer(dLedgerConfig, this.dLedgerRpcService);
        if (start) {
            dLedgerServer.startup();
        }
        return dLedgerServer;
    }

    public synchronized void removeDLedgerServer(DLedgerServer dLedgerServer) {
        this.configManager.removeDLedgerConfig(dLedgerServer.getdLedgerConfig().getSelfId());
        this.dLedgerManager.removeDLedgerServer(dLedgerServer);
        return;
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
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleAppend(request);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandleAppend] failed", e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleGet(request);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandleGet] failed", e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleMetadata(request);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandleMetaData] failed", e);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(LeadershipTransferRequest leadershipTransferRequest) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(leadershipTransferRequest.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", leadershipTransferRequest.getGroup(), leadershipTransferRequest.getRemoteId());
            return dLedgerServer.handleLeadershipTransfer(leadershipTransferRequest);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandleLeadershipTransfer] failed", e);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(leadershipTransferRequest);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleVote(request);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandleVote] failed", e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleHeartBeat(request);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandleHeartBeat] failed", e);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handlePull(request);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandlePull] failed", e);
            PullEntriesResponse response = new PullEntriesResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId()).handlePush(request);
        } catch (DLedgerException e) {
            logger.error("[Proxy][HandlePush] failed", e);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
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
        this.dLedgerRpcService.shutdown();
    }

    public NettyRemotingServer getRemotingServer() {
        if (this.dLedgerRpcService instanceof DLedgerRpcNettyService) {
            return ((DLedgerRpcNettyService) this.dLedgerRpcService).getRemotingServer();
        }
        return null;
    }

}
