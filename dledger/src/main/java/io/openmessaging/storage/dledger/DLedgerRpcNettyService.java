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

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import io.openmessaging.storage.dledger.common.NamedThreadFactory;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerRequestCode;
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
import io.openmessaging.storage.dledger.protocol.RequestOrResponse;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineCommandHeader;
import io.openmessaging.storage.dledger.protocol.VoteRequest;
import io.openmessaging.storage.dledger.protocol.VoteResponse;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineProcessor;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineRequest;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A netty implementation of DLedgerRpcService. It should be bi-directional, which means it implements both
 * DLedgerProtocol and DLedgerProtocolHandler.
 */

public class DLedgerRpcNettyService extends DLedgerRpcService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerRpcNettyService.class);

    private final NettyRemotingServer remotingServer;

    private final NettyRemotingClient remotingClient;

    private AbstractDLedgerServer dLedger;

    private final ConcurrentHashMap<Integer, UserDefineProcessor<? extends UserDefineRequest, ? extends UserDefineResponse>> userDefineProcessors = new ConcurrentHashMap<Integer, UserDefineProcessor<? extends UserDefineRequest, ? extends UserDefineResponse>>();

    private final ExecutorService futureExecutor = Executors.newFixedThreadPool(4, new NamedThreadFactory("FutureExecutor"));

    private final ExecutorService voteInvokeExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("voteInvokeExecutor"));

    private final ExecutorService heartBeatInvokeExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("heartBeatInvokeExecutor"));

    public DLedgerRpcNettyService(AbstractDLedgerServer dLedger) {
        this(dLedger, null, null, null);
    }

    public DLedgerRpcNettyService(AbstractDLedgerServer dLedger, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig) {
        this(dLedger, nettyServerConfig, nettyClientConfig, null);
    }

    public DLedgerRpcNettyService(AbstractDLedgerServer dLedger, NettyServerConfig nettyServerConfig, NettyClientConfig nettyClientConfig, ChannelEventListener channelEventListener) {
        this.dLedger = dLedger;
        NettyRequestProcessor protocolProcessor = new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                return DLedgerRpcNettyService.this.processRequest(ctx, request);
            }

            @Override
            public boolean rejectRequest() {
                return false;
            }
        };

        //register remoting server(We will only listen to one port. Limit in the configuration file)
        String address = this.dLedger.getListenAddress();
        if (nettyServerConfig == null) {
            nettyServerConfig = new NettyServerConfig();
        }
        nettyServerConfig.setListenPort(Integer.parseInt(address.split(":")[1]));
        this.remotingServer = registerRemotingServer(nettyServerConfig, channelEventListener, protocolProcessor);
        //start the remoting client
        if (nettyClientConfig == null) {
            nettyClientConfig = new NettyClientConfig();
        }
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
    }

    @Override
    public void registerUserDefineProcessor(UserDefineProcessor<? extends UserDefineRequest, ? extends UserDefineResponse> userDefineProcessor) {
        this.userDefineProcessors.put(userDefineProcessor.getRequestTypeCode(), userDefineProcessor);
    }

    private void registerProcessor(NettyRemotingServer remotingServer, NettyRequestProcessor protocolProcessor) {
        remotingServer.registerProcessor(DLedgerRequestCode.METADATA.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.APPEND.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.GET.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.PULL.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.PUSH.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.VOTE.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.HEART_BEAT.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.LEADERSHIP_TRANSFER.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.USER_DEFINE_REQUEST.getCode(), protocolProcessor, null);
        remotingServer.registerProcessor(DLedgerRequestCode.INSTALL_SNAPSHOT.getCode(), protocolProcessor, null);
    }

    private NettyRemotingServer registerRemotingServer(NettyServerConfig nettyServerConfig, ChannelEventListener channelEventListener, NettyRequestProcessor protocolProcessor) {
        NettyRemotingServer remotingServer = new NettyRemotingServer(nettyServerConfig, channelEventListener);
        registerProcessor(remotingServer, protocolProcessor);
        return remotingServer;
    }


    private String getPeerAddr(String groupId, String selfId) {
        return this.dLedger.getPeerAddr(groupId, selfId);
    }

    @Override
    public CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) {
        CompletableFuture<HeartBeatResponse> future = new CompletableFuture<>();
        heartBeatInvokeExecutor.execute(() -> {
            try {
                RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.HEART_BEAT.getCode(), null);
                wrapperRequest.setBody(JSON.toJSONBytes(request));
                remotingClient.invokeAsync(getPeerAddr(request.getGroup(), request.getRemoteId()), wrapperRequest, 3000, responseFuture -> {
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand != null) {
                        HeartBeatResponse response = JSON.parseObject(responseCommand.getBody(), HeartBeatResponse.class);
                        future.complete(response);
                    } else {
                        LOGGER.error("HeartBeat request time out, {}", request.baseInfo());
                        future.complete(new HeartBeatResponse().code(DLedgerResponseCode.NETWORK_ERROR.getCode()));
                    }
                });
            } catch (Throwable t) {
                LOGGER.error("Send heartBeat request failed, {}", request.baseInfo(), t);
                future.complete(new HeartBeatResponse().code(DLedgerResponseCode.NETWORK_ERROR.getCode()));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<VoteResponse> vote(VoteRequest request) {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        voteInvokeExecutor.execute(() -> {
            try {
                RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.VOTE.getCode(), null);
                wrapperRequest.setBody(JSON.toJSONBytes(request));
                this.remotingClient.invokeAsync(getPeerAddr(request.getGroup(), request.getRemoteId()), wrapperRequest, 3000, responseFuture -> {
                    RemotingCommand responseCommand = responseFuture.getResponseCommand();
                    if (responseCommand != null) {
                        VoteResponse response = JSON.parseObject(responseCommand.getBody(), VoteResponse.class);
                        future.complete(response);
                    } else {
                        LOGGER.error("Vote request time out, {}", request.baseInfo());
                        future.complete(new VoteResponse());
                    }
                });
            } catch (Throwable t) {
                LOGGER.error("Send vote request failed, {}", request.baseInfo(), t);
                future.complete(new VoteResponse());
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        GetEntriesResponse entriesResponse = new GetEntriesResponse();
        entriesResponse.setCode(DLedgerResponseCode.UNSUPPORTED.getCode());
        return CompletableFuture.completedFuture(entriesResponse);
    }

    @Override
    public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        CompletableFuture<AppendEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.APPEND.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request.getGroup(), request.getRemoteId()), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();

                AppendEntryResponse response;
                if (responseCommand != null) {
                    response = JSON.parseObject(responseCommand.getBody(), AppendEntryResponse.class);
                } else {
                    response = new AppendEntryResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                }
                future.complete(response);
            });
        } catch (Throwable t) {
            LOGGER.error("Send append request failed, {}", request.baseInfo(), t);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }
        return future;
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        MetadataResponse metadataResponse = new MetadataResponse();
        metadataResponse.setCode(DLedgerResponseCode.UNSUPPORTED.getCode());
        return CompletableFuture.completedFuture(metadataResponse);
    }

    @Override
    public CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.PULL.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = remotingClient.invokeSync(getPeerAddr(request.getGroup(), request.getRemoteId()), wrapperRequest, 3000);
        PullEntriesResponse response = JSON.parseObject(wrapperResponse.getBody(), PullEntriesResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<PushEntryResponse> push(PushEntryRequest request) {
        CompletableFuture<PushEntryResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.PUSH.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request.getGroup(), request.getRemoteId()), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();

                PushEntryResponse response;
                if (responseCommand != null) {
                    response = JSON.parseObject(responseCommand.getBody(), PushEntryResponse.class);
                } else {
                    response = new PushEntryResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                }
                future.complete(response);
            });
        } catch (Throwable t) {
            LOGGER.error("Send push request failed, {}", request.baseInfo(), t);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }

        return future;
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        CompletableFuture<InstallSnapshotResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.INSTALL_SNAPSHOT.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request.getGroup(), request.getRemoteId()), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();

                InstallSnapshotResponse response;
                if (responseCommand != null) {
                    response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), InstallSnapshotResponse.class);
                } else {
                    response = new InstallSnapshotResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                }
                future.complete(response);
            });
        } catch (Throwable t) {
            LOGGER.error("Send install snapshot request failed, {}", request.baseInfo(), t);
            InstallSnapshotResponse response = new InstallSnapshotResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }
        return future;
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> leadershipTransfer(
            LeadershipTransferRequest request) {
        CompletableFuture<LeadershipTransferResponse> future = new CompletableFuture<>();
        try {
            RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.LEADERSHIP_TRANSFER.getCode(), null);
            wrapperRequest.setBody(JSON.toJSONBytes(request));
            remotingClient.invokeAsync(getPeerAddr(request.getGroup(), request.getRemoteId()), wrapperRequest, 3000, responseFuture -> {
                RemotingCommand responseCommand = responseFuture.getResponseCommand();

                LeadershipTransferResponse response;
                if (responseCommand != null) {
                    response = JSON.parseObject(responseFuture.getResponseCommand().getBody(), LeadershipTransferResponse.class);
                } else {
                    response = new LeadershipTransferResponse();
                    response.copyBaseInfo(request);
                    response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
                }
                future.complete(response);
            });
        } catch (Throwable t) {
            LOGGER.error("Send leadershipTransfer request failed, {}", request.baseInfo(), t);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(DLedgerResponseCode.NETWORK_ERROR.getCode());
            future.complete(response);
        }

        return future;
    }

    private void writeResponse(RequestOrResponse storeResp, Throwable t, RemotingCommand request,
                               ChannelHandlerContext ctx) {
        RemotingCommand response = null;
        try {
            if (t != null) {
                //the t should not be null, using error code instead
                throw t;
            } else {
                response = handleResponse(storeResp, request);
                response.markResponseType();
                ctx.writeAndFlush(response);
            }
        } catch (Throwable e) {
            LOGGER.error("Process request over, but fire response failed, request:[{}] response:[{}]", request, response, e);
        }
    }

    /**
     * The core method to handle rpc requests. The advantages of using future instead of callback:
     * <p>
     * 1. separate the caller from actual executor, which make it able to handle the future results by the caller's wish
     * 2. simplify the later execution method
     * <p>
     * CompletableFuture is an excellent choice, whenCompleteAsync will handle the response asynchronously. With an
     * independent thread-pool, it will improve performance and reduce blocking points.
     *
     * @param ctx
     * @param request
     * @return
     * @throws Exception
     */
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        DLedgerRequestCode requestCode = DLedgerRequestCode.valueOf(request.getCode());
        switch (requestCode) {
            case METADATA: {
                MetadataRequest metadataRequest = JSON.parseObject(request.getBody(), MetadataRequest.class);
                CompletableFuture<MetadataResponse> future = handleMetadata(metadataRequest);
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            }
            case APPEND: {
                AppendEntryRequest appendEntryRequest = JSON.parseObject(request.getBody(), AppendEntryRequest.class);
                CompletableFuture<AppendEntryResponse> future = handleAppend(appendEntryRequest);
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            }
            case GET: {
                GetEntriesRequest getEntriesRequest = JSON.parseObject(request.getBody(), GetEntriesRequest.class);
                CompletableFuture<GetEntriesResponse> future = handleGet(getEntriesRequest);
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            }
            case PULL: {
                PullEntriesRequest pullEntriesRequest = JSON.parseObject(request.getBody(), PullEntriesRequest.class);
                CompletableFuture<PullEntriesResponse> future = handlePull(pullEntriesRequest);
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            }
            case PUSH: {
                PushEntryRequest pushEntryRequest = JSON.parseObject(request.getBody(), PushEntryRequest.class);
                CompletableFuture<PushEntryResponse> future = handlePush(pushEntryRequest);
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            }
            case VOTE: {
                VoteRequest voteRequest = JSON.parseObject(request.getBody(), VoteRequest.class);
                CompletableFuture<VoteResponse> future = handleVote(voteRequest);
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            }
            case HEART_BEAT: {
                HeartBeatRequest heartBeatRequest = JSON.parseObject(request.getBody(), HeartBeatRequest.class);
                CompletableFuture<HeartBeatResponse> future = handleHeartBeat(heartBeatRequest);
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            }
            case LEADERSHIP_TRANSFER: {
                long start = System.currentTimeMillis();
                LeadershipTransferRequest leadershipTransferRequest = JSON.parseObject(request.getBody(), LeadershipTransferRequest.class);
                CompletableFuture<LeadershipTransferResponse> future = handleLeadershipTransfer(leadershipTransferRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                    LOGGER.info("LEADERSHIP_TRANSFER FINISHED. Request={}, response={}, cost={}ms",
                            request, x, DLedgerUtils.elapsed(start));
                }, futureExecutor);
                break;
            }
            case INSTALL_SNAPSHOT: {
                InstallSnapshotRequest installSnapshotRequest = JSON.parseObject(request.getBody(), InstallSnapshotRequest.class);
                CompletableFuture<InstallSnapshotResponse> future = handleInstallSnapshot(installSnapshotRequest);
                future.whenCompleteAsync((x, y) -> {
                    writeResponse(x, y, request, ctx);
                }, futureExecutor);
                break;
            }
            case USER_DEFINE_REQUEST:
                UserDefineCommandHeader header = (UserDefineCommandHeader) request.decodeCommandCustomHeader(UserDefineCommandHeader.class);
                UserDefineProcessor<? extends UserDefineRequest, ? extends UserDefineResponse> userDefineProcessor = this.userDefineProcessors.get(header.getRequestTypeCode());
                if (userDefineProcessor == null) {
                    LOGGER.error("There is no processor to match this user-defined request type code: {}", header.getRequestTypeCode());
                    break;
                }
                CompletableFuture<? extends UserDefineResponse> future = userDefineProcessor.handleRequest(JSON.parseObject(request.getBody(), userDefineProcessor.getRequestType()));
                future.whenCompleteAsync((x, y) -> writeResponse(x, y, request, ctx), futureExecutor);
                break;
            default:
                LOGGER.error("Unknown request code {} from {}", request.getCode(), request);
                break;
        }
        return null;
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
            LeadershipTransferRequest leadershipTransferRequest) throws Exception {
        return this.dLedger.handleLeadershipTransfer(leadershipTransferRequest);
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return this.dLedger.handleHeartBeat(request);
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        return this.dLedger.handleVote(request);
    }

    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        return this.dLedger.handleAppend(request);
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return this.dLedger.handleGet(request);
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        return this.dLedger.handleMetadata(request);
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        return this.dLedger.handlePull(request);
    }

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return this.dLedger.handlePush(request);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> handleInstallSnapshot(InstallSnapshotRequest request) throws Exception {
        return this.dLedger.handleInstallSnapshot(request);
    }

    public RemotingCommand handleResponse(RequestOrResponse response, RemotingCommand request) {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(DLedgerResponseCode.SUCCESS.getCode(), null);
        remotingCommand.setBody(JSON.toJSONBytes(response));
        remotingCommand.setOpaque(request.getOpaque());
        return remotingCommand;
    }

    @Override
    public void startup() {
        this.remotingServer.start();
        this.remotingClient.start();
        LOGGER.info("listen the port: {}", this.remotingServer.localListenPort());
    }

    @Override
    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingClient.shutdown();
    }

    // only for test
    public void setDLedger(AbstractDLedgerServer dLedger) {
        this.dLedger = dLedger;
    }

    public NettyRemotingServer getRemotingServer() {
        return remotingServer;
    }

    public NettyRemotingClient getRemotingClient() {
        return remotingClient;
    }
}
