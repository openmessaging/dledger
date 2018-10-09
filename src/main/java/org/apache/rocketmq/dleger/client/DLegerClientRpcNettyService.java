package org.apache.rocketmq.dleger.client;

import com.alibaba.fastjson.JSON;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerRequestCode;
import org.apache.rocketmq.dleger.protocol.GetEntriesRequest;
import org.apache.rocketmq.dleger.protocol.GetEntriesResponse;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DLegerClientRpcNettyService extends DLegerClientRpcService {

    private NettyRemotingClient remotingClient;


    public DLegerClientRpcNettyService() {
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig(), null);
    }



    @Override
    public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.APPEND, null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        AppendEntryResponse  response = JSON.parseObject(wrapperResponse.getBody(), AppendEntryResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.GET, null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        GetEntriesResponse  response = JSON.parseObject(wrapperResponse.getBody(), GetEntriesResponse.class);
        return CompletableFuture.completedFuture(response);
    }


    @Override
    public void startup() {
        this.remotingClient.start();
    }
    @Override
    public void shutdown() {
        this.remotingClient.shutdown();
    }
}
