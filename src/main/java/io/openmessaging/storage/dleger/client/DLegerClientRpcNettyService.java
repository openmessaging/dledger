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

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dleger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dleger.protocol.DLegerRequestCode;
import io.openmessaging.storage.dleger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dleger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dleger.protocol.MetadataRequest;
import io.openmessaging.storage.dleger.protocol.MetadataResponse;
import java.util.concurrent.CompletableFuture;
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
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLegerRequestCode.APPEND.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        AppendEntryResponse response = JSON.parseObject(wrapperResponse.getBody(), AppendEntryResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLegerRequestCode.METADATA.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        MetadataResponse response = JSON.parseObject(wrapperResponse.getBody(), MetadataResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLegerRequestCode.GET.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        GetEntriesResponse response = JSON.parseObject(wrapperResponse.getBody(), GetEntriesResponse.class);
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
