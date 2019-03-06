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

package io.openmessaging.storage.dledger.client;

import com.alibaba.fastjson.JSON;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.DLedgerRequestCode;
import io.openmessaging.storage.dledger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dledger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dledger.protocol.MetadataRequest;
import io.openmessaging.storage.dledger.protocol.MetadataResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferRequest;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class DLedgerClientRpcNettyService extends DLedgerClientRpcService {

    private NettyRemotingClient remotingClient;

    public DLedgerClientRpcNettyService() {
        this.remotingClient = new NettyRemotingClient(new NettyClientConfig(), null);
    }

    @Override
    public CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.APPEND.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        AppendEntryResponse response = JSON.parseObject(wrapperResponse.getBody(), AppendEntryResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.METADATA.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 3000);
        MetadataResponse response = JSON.parseObject(wrapperResponse.getBody(), MetadataResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> leadershipTransfer(LeadershipTransferRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.LEADERSHIP_TRANSFER.getCode(), null);
        wrapperRequest.setBody(JSON.toJSONBytes(request));
        RemotingCommand wrapperResponse = this.remotingClient.invokeSync(getPeerAddr(request.getRemoteId()), wrapperRequest, 10000);
        LeadershipTransferResponse response = JSON.parseObject(wrapperResponse.getBody(), LeadershipTransferResponse.class);
        return CompletableFuture.completedFuture(response);
    }

    @Override
    public CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception {
        RemotingCommand wrapperRequest = RemotingCommand.createRequestCommand(DLedgerRequestCode.GET.getCode(), null);
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
