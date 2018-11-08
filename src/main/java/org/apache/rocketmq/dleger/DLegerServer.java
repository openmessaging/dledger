package org.apache.rocketmq.dleger;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerProtocolHander;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.GetEntriesRequest;
import org.apache.rocketmq.dleger.protocol.GetEntriesResponse;
import org.apache.rocketmq.dleger.protocol.HeartBeatRequest;
import org.apache.rocketmq.dleger.protocol.HeartBeatResponse;
import org.apache.rocketmq.dleger.protocol.MetadataRequest;
import org.apache.rocketmq.dleger.protocol.MetadataResponse;
import org.apache.rocketmq.dleger.protocol.PullEntriesRequest;
import org.apache.rocketmq.dleger.protocol.PullEntriesResponse;
import org.apache.rocketmq.dleger.protocol.PushEntryRequest;
import org.apache.rocketmq.dleger.protocol.PushEntryResponse;
import org.apache.rocketmq.dleger.protocol.VoteRequest;
import org.apache.rocketmq.dleger.protocol.VoteResponse;
import org.apache.rocketmq.dleger.store.DLegerMemoryStore;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.apache.rocketmq.dleger.store.file.DLegerMmapFileStore;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerServer implements DLegerProtocolHander {


    private Logger logger = LoggerFactory.getLogger(DLegerServer.class);

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
                return  dLegerEntryPusher.waitAck(resEntry);
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
