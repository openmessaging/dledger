package io.openmessaging.storage.dleger.client;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dleger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dleger.protocol.DLegerRequestCode;
import io.openmessaging.storage.dleger.protocol.MetadataResponse;
import java.util.concurrent.CompletableFuture;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dleger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dleger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dleger.protocol.MetadataRequest;
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
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.APPEND.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        AppendEntryResponse  response = JSON.parseObject(wrapperResponse.getBody(), AppendEntryResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.METADATA.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        MetadataResponse  response = JSON.parseObject(wrapperResponse.getBody(), MetadataResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest =  RemotingCommand.createRequestCommand(DLegerRequestCode.GET.getCode(), null);
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
