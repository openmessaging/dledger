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

package io.openmessaging.storage.dleger;

import io.openmessaging.storage.dleger.entry.DLegerEntry;
import io.openmessaging.storage.dleger.exception.DLegerException;
import io.openmessaging.storage.dleger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dleger.protocol.DLegerProtocolHander;
import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;
import io.openmessaging.storage.dleger.protocol.GetEntriesRequest;
import io.openmessaging.storage.dleger.protocol.GetEntriesResponse;
import io.openmessaging.storage.dleger.protocol.HeartBeatRequest;
import io.openmessaging.storage.dleger.protocol.HeartBeatResponse;
import io.openmessaging.storage.dleger.protocol.MetadataRequest;
import io.openmessaging.storage.dleger.protocol.MetadataResponse;
import io.openmessaging.storage.dleger.protocol.PullEntriesRequest;
import io.openmessaging.storage.dleger.protocol.PullEntriesResponse;
import io.openmessaging.storage.dleger.protocol.PushEntryRequest;
import io.openmessaging.storage.dleger.protocol.PushEntryResponse;
import io.openmessaging.storage.dleger.protocol.VoteRequest;
import io.openmessaging.storage.dleger.protocol.VoteResponse;
import io.openmessaging.storage.dleger.store.DLegerMemoryStore;
import io.openmessaging.storage.dleger.store.DLegerStore;
import io.openmessaging.storage.dleger.store.file.DLegerMmapFileStore;
import io.openmessaging.storage.dleger.utils.PreConditions;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerServer implements DLegerProtocolHander {

    private static Logger logger = LoggerFactory.getLogger(DLegerServer.class);

    private MemberState memberState;
    private DLegerConfig dLegerConfig;

    private DLegerStore dLegerStore;
    private DLegerRpcService dLegerRpcService;
    private DLegerEntryPusher dLegerEntryPusher;
    private DLegerLeaderElector dLegerLeaderElector;

    public DLegerServer(DLegerConfig dLegerConfig) {
        this.dLegerConfig = dLegerConfig;
        this.memberState = new MemberState(dLegerConfig);
        if (dLegerConfig.getStoreType().equals(DLegerConfig.MEMORY)) {
            dLegerStore = new DLegerMemoryStore(this.dLegerConfig, this.memberState);
        } else {
            dLegerStore = new DLegerMmapFileStore(this.dLegerConfig, this.memberState);
        }
        dLegerRpcService = new DLegerRpcNettyService(this);
        dLegerEntryPusher = new DLegerEntryPusher(dLegerConfig, memberState, dLegerStore, dLegerRpcService);
        dLegerLeaderElector = new DLegerLeaderElector(dLegerConfig, memberState, dLegerRpcService);
    }

    public void startup() {
        this.dLegerStore.startup();
        this.dLegerRpcService.startup();
        this.dLegerEntryPusher.startup();
        this.dLegerLeaderElector.startup();
    }

    public void shutdown() {
        this.dLegerLeaderElector.shutdown();
        this.dLegerEntryPusher.shutdown();
        this.dLegerRpcService.shutdown();
        this.dLegerStore.shutdown();
    }

    public MemberState getMemberState() {
        return memberState;
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        try {

            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLegerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLegerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLegerLeaderElector.handleHeartBeat(request);
        } catch (DLegerException e) {
            logger.error("[{}][HandleHeartBeat] failed", memberState.getSelfId(), e);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLegerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLegerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLegerLeaderElector.handleVote(request, false);
        } catch (DLegerException e) {
            logger.error("[{}][HandleVote] failed", memberState.getSelfId(), e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws IOException {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLegerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLegerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLegerResponseCode.NOT_LEADER);
            long currTerm = memberState.currTerm();
            if (dLegerEntryPusher.isPendingFull(currTerm)) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setGroup(memberState.getGroup());
                appendEntryResponse.setCode(DLegerResponseCode.LEADER_PENDING_FULL.getCode());
                appendEntryResponse.setTerm(currTerm);
                appendEntryResponse.setLeaderId(memberState.getSelfId());
                return CompletableFuture.completedFuture(appendEntryResponse);
            } else {
                DLegerEntry dLegerEntry = new DLegerEntry();
                dLegerEntry.setBody(request.getBody());
                DLegerEntry resEntry = dLegerStore.appendAsLeader(dLegerEntry);
                return dLegerEntryPusher.waitAck(resEntry);
            }
        } catch (DLegerException e) {
            logger.error("[{}][HandleAppend] failed", memberState.getSelfId(), e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws IOException {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLegerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLegerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLegerResponseCode.NOT_LEADER);
            DLegerEntry entry = dLegerStore.get(request.getBeginIndex());
            GetEntriesResponse response = new GetEntriesResponse();
            response.setGroup(memberState.getGroup());
            if (entry != null) {
                response.setEntries(Collections.singletonList(entry));
            }
            return CompletableFuture.completedFuture(response);
        } catch (DLegerException e) {
            logger.error("[{}][HandleGet] failed", memberState.getSelfId(), e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setLeaderId(memberState.getLeaderId());
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLegerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLegerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            MetadataResponse metadataResponse = new MetadataResponse();
            metadataResponse.setGroup(memberState.getGroup());
            metadataResponse.setPeers(memberState.getPeerMap());
            metadataResponse.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(metadataResponse);
        } catch (DLegerException e) {
            logger.error("[{}][HandleMetadata] failed", memberState.getSelfId(), e);
            MetadataResponse response = new MetadataResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    @Override
    public CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) {
        return null;
    }

    @Override public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLegerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLegerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLegerEntryPusher.handlePush(request);
        } catch (DLegerException e) {
            logger.error("[{}][HandlePush] failed", memberState.getSelfId(), e);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    public DLegerStore getdLegerStore() {
        return dLegerStore;
    }

    public DLegerRpcService getdLegerRpcService() {
        return dLegerRpcService;
    }

    public DLegerLeaderElector getdLegerLeaderElector() {
        return dLegerLeaderElector;
    }

    public DLegerConfig getdLegerConfig() {
        return dLegerConfig;
    }
}
