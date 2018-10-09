package org.apache.rocketmq.dleger;

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerProtocolHander;
import org.apache.rocketmq.dleger.protocol.DLegerRequestCode;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.GetEntriesRequest;
import org.apache.rocketmq.dleger.protocol.GetEntriesResponse;
import org.apache.rocketmq.dleger.protocol.HeartBeatRequest;
import org.apache.rocketmq.dleger.protocol.HeartBeatResponse;
import org.apache.rocketmq.dleger.protocol.PullEntriesRequest;
import org.apache.rocketmq.dleger.protocol.PullEntriesResponse;
import org.apache.rocketmq.dleger.protocol.PushEntryRequest;
import org.apache.rocketmq.dleger.protocol.PushEntryResponse;
import org.apache.rocketmq.dleger.protocol.RequestOrResponse;
import org.apache.rocketmq.dleger.protocol.VoteRequest;
import org.apache.rocketmq.dleger.protocol.VoteResponse;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The rpc service should be bi-directional.
 *
 */

public class DLegerRpcNettyService  extends DLegerRpcService {

    private Logger logger = LoggerFactory.getLogger(DLegerRpcNettyService.class);

    private NettyRemotingServer remotingServer;
    private NettyRemotingClient remotingClient;


    private MemberState memberState;

    private DLegerServer dLegerServer;





    public DLegerRpcNettyService(DLegerServer dLegerServer) {
        this.dLegerServer = dLegerServer;
        this.memberState = dLegerServer.getMemberState();
        NettyRequestProcessor protocolProcessor = new NettyRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                return DLegerRpcNettyService.this.processRequest(ctx, request);
            }

            @Override public boolean rejectRequest() {
                return false;
            }
        };
        //start the remoting server
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(Integer.valueOf(memberState.getSelfAddr().split(":")[1]));
        this.remotingServer = new NettyRemotingServer(nettyServerConfig, null);
        this.remotingServer.registerProcessor(DLegerRequestCode.APPEND, protocolProcessor, null);
        this.remotingServer.registerProcessor(DLegerRequestCode.GET, protocolProcessor, null);
        this.remotingServer.registerProcessor(DLegerRequestCode.PULL, protocolProcessor, null);
        this.remotingServer.registerProcessor(DLegerRequestCode.PUSH, protocolProcessor, null);
        this.remotingServer.registerProcessor(DLegerRequestCode.VOTE, protocolProcessor, null);
        this.remotingServer.registerProcessor(DLegerRequestCode.HEART_BEAT, protocolProcessor, null);

        //start the remoting client
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig(), null);


    }

    @Override public CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.HEART_BEAT, null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        remotingClient.invokeOneway(memberState.getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        return null;
    }

    @Override public CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.VOTE, null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = remotingClient.invokeSync(memberState.getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        VoteResponse  response = JSON.parseObject(wrapperResponse.getBody(), VoteResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        return null;
    }

    @Override public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.APPEND, null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = remotingClient.invokeSync(memberState.getLeaderAddr(), wrapperRequest, 3000);
        AppendEntryResponse  response = JSON.parseObject(wrapperResponse.getBody(), AppendEntryResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override public CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.PULL, null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = remotingClient.invokeSync(memberState.getLeaderAddr(), wrapperRequest, 3000);
        PullEntriesResponse  response = JSON.parseObject(wrapperResponse.getBody(), PullEntriesResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override public CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.PUSH, null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = remotingClient.invokeSync(memberState.getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        PushEntryResponse  response = JSON.parseObject(wrapperResponse.getBody(), PushEntryResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case DLegerRequestCode.APPEND:
                AppendEntryRequest appendEntryRequest = JSON.parseObject(request.getBody(), AppendEntryRequest.class);
                return handleResponse(handleAppend(appendEntryRequest).get(), request);
            case DLegerRequestCode.GET:
                GetEntriesRequest getEntriesRequest = JSON.parseObject(request.getBody(), GetEntriesRequest.class);
                return handleResponse(handleGet(getEntriesRequest).get(), request);
            case DLegerRequestCode.PULL:
                PullEntriesRequest pullEntriesRequest = JSON.parseObject(request.getBody(), PullEntriesRequest.class);
                return handleResponse(handlePull(pullEntriesRequest).get(), request);
            case DLegerRequestCode.PUSH:
                PushEntryRequest pushEntryRequest = JSON.parseObject(request.getBody(), PushEntryRequest.class);
                return handleResponse(handlePush(pushEntryRequest).get(), request);
            case DLegerRequestCode.VOTE:
                VoteRequest voteRequest = JSON.parseObject(request.getBody(), VoteRequest.class);
                return handleResponse(handleVote(voteRequest).get(), request);
            case DLegerRequestCode.HEART_BEAT:
                HeartBeatRequest heartBeatRequest = JSON.parseObject(request.getBody(), HeartBeatRequest.class);
                return handleResponse(handleHeartBeat(heartBeatRequest).get(), request);
            default:
                break;
        }
        return null;
    }


    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        return dLegerServer.handleHeartBeat(request);
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        VoteResponse response = dLegerServer.handleVote(request).get();
        logger.info("[{}][HandleVote_{}] {} handleVote for {} in term {}", memberState.getSelfId(), response.getVoteResult(), memberState.getSelfId(), request.getLeaderId(), request.getCurrTerm());
        return CompletableFuture.completedFuture(response);
    }


    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception {
        return dLegerServer.handleAppend(request);
    }

    @Override public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception {
        return dLegerServer.handleGet(request);
    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception {
        return dLegerServer.handlePull(request);
    }

    @Override public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        return dLegerServer.handlePush(request);
    }

    public RemotingCommand handleResponse(RequestOrResponse response, RemotingCommand request) {
        RemotingCommand remotingCommand = RemotingCommand.createResponseCommand(DLegerResponseCode.SUCCESS, null);
        remotingCommand.setBody(JSON.toJSONBytes(response));
        remotingCommand.setOpaque(request.getOpaque());
        return remotingCommand;
    }


    @Override
    public void startup() {
        this.remotingServer.start();
        this.remotingClient.start();
    }

    @Override
    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingClient.shutdown();
    }

}
