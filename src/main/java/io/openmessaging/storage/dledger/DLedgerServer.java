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

import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
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
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.PreConditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerServer implements DLedgerProtocolHandler {

    private static Logger logger = LoggerFactory.getLogger(DLedgerServer.class);

    private MemberState memberState;
    private DLedgerConfig dLedgerConfig;

    private DLedgerStore dLedgerStore;
    private DLedgerRpcService dLedgerRpcService;
    private DLedgerEntryPusher dLedgerEntryPusher;
    private DLedgerLeaderElector dLedgerLeaderElector;

    private ScheduledExecutorService executorService;
    private Optional<StateMachineCaller> fsmCaller;

    public DLedgerServer(DLedgerConfig dLedgerConfig) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = new MemberState(dLedgerConfig);
        this.dLedgerStore = createDLedgerStore(dLedgerConfig.getStoreType(), this.dLedgerConfig, this.memberState);
        dLedgerRpcService = new DLedgerRpcNettyService(this);
        dLedgerEntryPusher = new DLedgerEntryPusher(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);
        dLedgerLeaderElector = new DLedgerLeaderElector(dLedgerConfig, memberState, dLedgerRpcService);
        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("DLedgerServer-ScheduledExecutor");
            return t;
        });
        this.fsmCaller = Optional.empty();
    }

    public void startup() {
        this.dLedgerStore.startup();
        this.dLedgerRpcService.startup();
        this.dLedgerEntryPusher.startup();
        this.dLedgerLeaderElector.startup();
        executorService.scheduleAtFixedRate(this::checkPreferredLeader, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        this.dLedgerLeaderElector.shutdown();
        this.dLedgerEntryPusher.shutdown();
        this.dLedgerRpcService.shutdown();
        this.dLedgerStore.shutdown();
        executorService.shutdown();
        this.fsmCaller.ifPresent(StateMachineCaller::shutdown);
    }

    private DLedgerStore createDLedgerStore(String storeType, DLedgerConfig config, MemberState memberState) {
        if (storeType.equals(DLedgerConfig.MEMORY)) {
            return new DLedgerMemoryStore(config, memberState);
        } else {
            return new DLedgerMmapFileStore(config, memberState);
        }
    }

    public MemberState getMemberState() {
        return memberState;
    }

    public void registerStateMachine(final StateMachine fsm) {
        final StateMachineCaller fsmCaller = new StateMachineCaller(this.dLedgerStore, fsm, this.dLedgerEntryPusher);
        fsmCaller.start();
        this.fsmCaller = Optional.of(fsmCaller);
        // Register state machine caller to entry pusher
        this.dLedgerEntryPusher.registerStateMachine(this.fsmCaller);
    }

    public StateMachine getStateMachine() {
        return this.fsmCaller.map(StateMachineCaller::getStateMachine).orElse(null);
    }

    @Override public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        try {

            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleHeartBeat(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleHeartBeat] failed", memberState.getSelfId(), e);
            HeartBeatResponse response = new HeartBeatResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override public CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleVote(request, false);
        } catch (DLedgerException e) {
            logger.error("[{}][HandleVote] failed", memberState.getSelfId(), e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    /**
     * Handle the append requests:
     * 1.append the entry to local store
     * 2.submit the future to entry pusher and wait the quorum ack
     * 3.if the pending requests are full, then reject it immediately
     *
     * @param request
     * @return
     * @throws IOException
     */
    @Override
    public CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws IOException {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            PreConditions.check(memberState.getTransferee() == null, DLedgerResponseCode.LEADER_TRANSFERRING);
            long currTerm = memberState.currTerm();
            if (dLedgerEntryPusher.isPendingFull(currTerm)) {
                AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
                appendEntryResponse.setGroup(memberState.getGroup());
                appendEntryResponse.setCode(DLedgerResponseCode.LEADER_PENDING_FULL.getCode());
                appendEntryResponse.setTerm(currTerm);
                appendEntryResponse.setLeaderId(memberState.getSelfId());
                return AppendFuture.newCompletedFuture(-1, appendEntryResponse);
            } else {
                if (request instanceof BatchAppendEntryRequest) {
                    BatchAppendEntryRequest batchRequest = (BatchAppendEntryRequest) request;
                    if (batchRequest.getBatchMsgs() != null && batchRequest.getBatchMsgs().size() != 0) {
                        // record positions to return;
                        long[] positions = new long[batchRequest.getBatchMsgs().size()];
                        DLedgerEntry resEntry = null;
                        // split bodys to append
                        int index = 0;
                        Iterator<byte[]> iterator = batchRequest.getBatchMsgs().iterator();
                        while (iterator.hasNext()) {
                            DLedgerEntry dLedgerEntry = new DLedgerEntry();
                            dLedgerEntry.setBody(iterator.next());
                            resEntry = dLedgerStore.appendAsLeader(dLedgerEntry);
                            positions[index++] = resEntry.getPos();
                        }
                        // only wait last entry ack is ok
                        BatchAppendFuture<AppendEntryResponse> batchAppendFuture =
                            (BatchAppendFuture<AppendEntryResponse>) dLedgerEntryPusher.waitAck(resEntry, true);
                        batchAppendFuture.setPositions(positions);
                        return batchAppendFuture;
                    }
                    throw new DLedgerException(DLedgerResponseCode.REQUEST_WITH_EMPTY_BODYS, "BatchAppendEntryRequest" +
                        " with empty bodys");
                } else {
                    DLedgerEntry dLedgerEntry = new DLedgerEntry();
                    dLedgerEntry.setBody(request.getBody());
                    DLedgerEntry resEntry = dLedgerStore.appendAsLeader(dLedgerEntry);
                    return dLedgerEntryPusher.waitAck(resEntry, false);
                }
            }
        } catch (DLedgerException e) {
            logger.error("[{}][HandleAppend] failed", memberState.getSelfId(), e);
            AppendEntryResponse response = new AppendEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return AppendFuture.newCompletedFuture(-1, response);
        }
    }

    @Override
    public CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws IOException {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER);
            DLedgerEntry entry = dLedgerStore.get(request.getBeginIndex());
            GetEntriesResponse response = new GetEntriesResponse();
            response.setGroup(memberState.getGroup());
            if (entry != null) {
                response.setEntries(Collections.singletonList(entry));
            }
            return CompletableFuture.completedFuture(response);
        } catch (DLedgerException e) {
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
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            MetadataResponse metadataResponse = new MetadataResponse();
            metadataResponse.setGroup(memberState.getGroup());
            metadataResponse.setPeers(memberState.getPeerMap());
            metadataResponse.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(metadataResponse);
        } catch (DLedgerException e) {
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
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerEntryPusher.handlePush(request);
        } catch (DLedgerException e) {
            logger.error("[{}][HandlePush] failed", memberState.getSelfId(), e);
            PushEntryResponse response = new PushEntryResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    @Override
    public CompletableFuture<LeadershipTransferResponse> handleLeadershipTransfer(
        LeadershipTransferRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            if (memberState.getSelfId().equals(request.getTransferId())) {
                //It's the leader received the transfer command.
                PreConditions.check(memberState.isPeerMember(request.getTransfereeId()), DLedgerResponseCode.UNKNOWN_MEMBER, "transferee=%s is not a peer member", request.getTransfereeId());
                PreConditions.check(memberState.currTerm() == request.getTerm(), DLedgerResponseCode.INCONSISTENT_TERM, "currTerm(%s) != request.term(%s)", memberState.currTerm(), request.getTerm());
                PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER, "selfId=%s is not leader=%s", memberState.getSelfId(), memberState.getLeaderId());

                // check fall transferee not fall behind much.
                long transfereeFallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(request.getTerm(), request.getTransfereeId());
                PreConditions.check(transfereeFallBehind < dLedgerConfig.getMaxLeadershipTransferWaitIndex(),
                    DLedgerResponseCode.FALL_BEHIND_TOO_MUCH, "transferee fall behind too much, diff=%s", transfereeFallBehind);
                return dLedgerLeaderElector.handleLeadershipTransfer(request);
            } else if (memberState.getSelfId().equals(request.getTransfereeId())) {
                // It's the transferee received the take leadership command.
                PreConditions.check(request.getTransferId().equals(memberState.getLeaderId()), DLedgerResponseCode.INCONSISTENT_LEADER, "transfer=%s is not leader", request.getTransferId());

                long costTime = 0L;
                long startTime = System.currentTimeMillis();
                long fallBehind = request.getTakeLeadershipLedgerIndex() - memberState.getLedgerEndIndex();

                while (fallBehind > 0) {

                    if (costTime > dLedgerConfig.getLeadershipTransferWaitTimeout()) {
                        throw new DLedgerException(DLedgerResponseCode.TAKE_LEADERSHIP_FAILED,
                            "transferee fall behind, wait timeout. timeout = {}, diff = {}",
                            dLedgerConfig.getLeadershipTransferWaitTimeout(), fallBehind);
                    }

                    logger.warn("transferee fall behind, diff = {}", fallBehind);
                    Thread.sleep(10);

                    fallBehind = request.getTakeLeadershipLedgerIndex() - memberState.getLedgerEndIndex();
                    costTime = System.currentTimeMillis() - startTime;
                }

                return dLedgerLeaderElector.handleTakeLeadership(request);
            } else {
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
            }
        } catch (DLedgerException e) {
            logger.error("[{}][handleLeadershipTransfer] failed", memberState.getSelfId(), e);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    private void checkPreferredLeader() {
        if (!memberState.isLeader()) {
            return;
        }

        if (dLedgerConfig.getPreferredLeaderIds() == null) {
            return;
        }

        if (memberState.getTransferee() != null) {
            return;
        }

        List<String> preferredLeaderIds = new ArrayList<>(Arrays.asList(dLedgerConfig.getPreferredLeaderIds().split(";")));
        if (preferredLeaderIds.contains(dLedgerConfig.getSelfId())) {
            return;
        }

        Iterator<String> it = preferredLeaderIds.iterator();
        while (it.hasNext()) {
            String preferredLeaderId = it.next();
            if (!memberState.isPeerMember(preferredLeaderId)) {
                it.remove();
                logger.warn("preferredLeaderId = {} is not a peer member", preferredLeaderId);
                continue;
            }

            if (!memberState.getPeersLiveTable().containsKey(preferredLeaderId) ||
                memberState.getPeersLiveTable().get(preferredLeaderId) == Boolean.FALSE.booleanValue()) {
                it.remove();
                logger.warn("preferredLeaderId = {} is not online", preferredLeaderId);
                continue;
            }

            long fallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(memberState.currTerm(), preferredLeaderId);
            if (fallBehind >= dLedgerConfig.getMaxLeadershipTransferWaitIndex()) {
                logger.warn("preferredLeaderId = {} transferee fall behind index : {}", preferredLeaderId, fallBehind);
                continue;
            }
        }

        if (preferredLeaderIds.size() == 0) {
            return;
        }
        long minFallBehind = Long.MAX_VALUE;
        String preferredLeaderId = preferredLeaderIds.get(0);
        for (String peerId : preferredLeaderIds) {
            long fallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(memberState.currTerm(), peerId);
            if (fallBehind < minFallBehind) {
                minFallBehind = fallBehind;
                preferredLeaderId = peerId;
            }
        }
        logger.info("preferredLeaderId = {}, which has the smallest fall behind index = {} and is decided to be transferee.", preferredLeaderId, minFallBehind);

        if (minFallBehind < dLedgerConfig.getMaxLeadershipTransferWaitIndex()) {
            LeadershipTransferRequest request = new LeadershipTransferRequest();
            request.setTerm(memberState.currTerm());
            request.setTransfereeId(preferredLeaderId);

            try {
                long startTransferTime = System.currentTimeMillis();
                LeadershipTransferResponse response = dLedgerLeaderElector.handleLeadershipTransfer(request).get();
                logger.info("transfer finished. request={},response={},cost={}ms", request, response, DLedgerUtils.elapsed(startTransferTime));
            } catch (Throwable t) {
                logger.error("[checkPreferredLeader] error, request={}", request, t);
            }
        }
    }

    public DLedgerStore getdLedgerStore() {
        return dLedgerStore;
    }

    public DLedgerRpcService getdLedgerRpcService() {
        return dLedgerRpcService;
    }

    public DLedgerLeaderElector getdLedgerLeaderElector() {
        return dLedgerLeaderElector;
    }

    public DLedgerConfig getdLedgerConfig() {
        return dLedgerConfig;
    }
}
