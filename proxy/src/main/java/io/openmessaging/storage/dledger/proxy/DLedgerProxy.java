/**
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
package io.openmessaging.storage.dledger.proxy;

import io.openmessaging.storage.dledger.common.AppendFuture;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerRpcNettyService;
import io.openmessaging.storage.dledger.DLedgerRpcService;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.AbstractDLedgerServer;
import io.openmessaging.storage.dledger.common.ReadClosure;
import io.openmessaging.storage.dledger.common.ReadMode;
import io.openmessaging.storage.dledger.common.WriteClosure;
import io.openmessaging.storage.dledger.common.WriteTask;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dledger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotRequest;
import io.openmessaging.storage.dledger.protocol.InstallSnapshotResponse;
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
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import io.openmessaging.storage.dledger.utils.PreConditions;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * DLedgerServers' proxy, which can handle the request and route it to different DLedgerServer
 */
public class DLedgerProxy extends AbstractDLedgerServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerProxy.class);

    private DLedgerManager dLedgerManager;

    private ConfigManager configManager;

    private DLedgerRpcService dLedgerRpcService;

    public DLedgerProxy(DLedgerConfig dLedgerConfig) {
        this(Collections.singletonList(dLedgerConfig));
    }

    public DLedgerProxy(DLedgerProxyConfig dLedgerProxyConfig) {
        this(dLedgerProxyConfig.getConfigs());
    }

    public DLedgerProxy(List<DLedgerConfig> dLedgerConfigs) {
        this(dLedgerConfigs, null, null, null);
    }

    public DLedgerProxy(List<DLedgerConfig> dLedgerConfigs, NettyServerConfig nettyServerConfig,
        NettyClientConfig nettyClientConfig) {
        this(dLedgerConfigs, nettyServerConfig, nettyClientConfig, null);
    }

    public DLedgerProxy(List<DLedgerConfig> dLedgerConfigs, NettyServerConfig nettyServerConfig,
        NettyClientConfig nettyClientConfig, ChannelEventListener channelEventListener) {
        try {
            this.configManager = new ConfigManager(dLedgerConfigs);
            this.dLedgerRpcService = new DLedgerRpcNettyService(this, nettyServerConfig, nettyClientConfig, channelEventListener);
            this.dLedgerManager = new DLedgerManager(this.configManager, this.dLedgerRpcService);
        } catch (Exception e) {
            LOGGER.error("[Proxy][DLedgerProxy] fail to construct", e);
            System.exit(-1);
        }
    }

    /**
     * initialize a DLedgerServer in this DLedgerProxy
     *
     * @param dLedgerConfig config of DLedgerServer need to add
     */
    public synchronized void addDLedgerServer(DLedgerConfig dLedgerConfig) {
        this.configManager.addDLedgerConfig(dLedgerConfig);
    }

    public synchronized void removeDLedgerServer(String groupId, String selfId) {
        this.configManager.removeDLedgerConfig(groupId, selfId);
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
            return dLedgerServer.handleAppend(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandleAppend] failed", e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleGet(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandleGet] failed", e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleMetadata(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandleMetaData] failed", e);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
        LeadershipTransferRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleLeadershipTransfer(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandleLeadershipTransfer] failed", e);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleVote(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandleVote] failed", e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handleHeartBeat(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandleHeartBeat] failed", e);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handlePull(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandlePull] failed", e);
            PullEntriesResponse response = new PullEntriesResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        DLedgerServer dLedgerServer = this.dLedgerManager.getDLedgerServer(request.getGroup(), request.getRemoteId());
        try {
            PreConditions.check(dLedgerServer != null, DLedgerResponseCode.UNKNOWN_MEMBER, "group[%s] selfId[%s] not exist in proxy", request.getGroup(), request.getRemoteId());
            return dLedgerServer.handlePush(request);
        } catch (DLedgerException e) {
            LOGGER.error("[Proxy][HandlePush] failed", e);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> handleInstallSnapshot(InstallSnapshotRequest request) throws Exception {
        return null;
    }

    public void startup() {
        this.dLedgerRpcService.startup();
        this.dLedgerManager.startup();
    }


    public DLedgerRpcService getDLedgerRpcService() {
        return dLedgerRpcService;
    }

    public void setDLedgerRpcService(DLedgerRpcService dLedgerRpcService) {
        this.dLedgerRpcService = dLedgerRpcService;
    }

    public void shutdown() {
        this.dLedgerManager.shutdown();
        this.dLedgerRpcService.shutdown();
    }

    @Override
    public String getListenAddress() {
        return this.configManager.getListenAddress();
    }

    @Override
    public String getPeerAddr(String groupId, String selfId) {
        return this.configManager.getAddress(groupId, selfId);
    }

    public void registerStateMachine(final StateMachine stateMachine) {
        this.dLedgerManager.registerStateMachine(stateMachine);
    }

    @Override
    public NettyRemotingClient getRemotingClient() {
        if (this.dLedgerRpcService instanceof DLedgerRpcNettyService) {
            return ((DLedgerRpcNettyService) this.dLedgerRpcService).getRemotingClient();
        }
        return null;
    }

    @Override
    public NettyRemotingServer getRemotingServer() {
        if (this.dLedgerRpcService instanceof DLedgerRpcNettyService) {
            return ((DLedgerRpcNettyService) this.dLedgerRpcService).getRemotingServer();
        }
        return null;
    }

    @Override
    public void handleRead(ReadMode mode, ReadClosure closure) {

    }

    @Override
    public void handleWrite(WriteTask task, WriteClosure closure) {
        
    }
}
