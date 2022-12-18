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

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import io.openmessaging.storage.dledger.protocol.BatchAppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.ChangePeersRequest;
import io.openmessaging.storage.dledger.protocol.ChangePeersResponse;
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
import io.openmessaging.storage.dledger.snapshot.SnapshotManager;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import io.openmessaging.storage.dledger.statemachine.StateMachineCaller;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.store.DLedgerStore;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import io.openmessaging.storage.dledger.utils.PreConditions;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DLedgerServer extends AbstractDLedgerServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerServer.class);

    private final MemberState memberState;

    private final DLedgerConfig dLedgerConfig;

    private final DLedgerStore dLedgerStore;

    private final DLedgerRpcService dLedgerRpcService;

    private final RpcServiceMode rpcServiceMode;

    private DLedgerEntryPusher dLedgerEntryPusher;

    private DLedgerLeaderElector dLedgerLeaderElector;

    private final ScheduledExecutorService executorService;

    private Optional<StateMachineCaller> fsmCaller;

    private volatile boolean isStarted = false;

    public DLedgerServer(DLedgerConfig dLedgerConfig) {
        this(dLedgerConfig, null, null, null);
    }

    public DLedgerServer(DLedgerConfig dLedgerConfig, NettyServerConfig nettyServerConfig) {
        this(dLedgerConfig, nettyServerConfig, null, null);
    }

    public DLedgerServer(DLedgerConfig dLedgerConfig, NettyServerConfig nettyServerConfig,
                         NettyClientConfig nettyClientConfig) {
        this(dLedgerConfig, nettyServerConfig, nettyClientConfig, null);
    }

    public DLedgerServer(DLedgerConfig dLedgerConfig, NettyServerConfig nettyServerConfig,
                         NettyClientConfig nettyClientConfig, ChannelEventListener channelEventListener) {
        dLedgerConfig.init();
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = new MemberState(dLedgerConfig);
        this.dLedgerStore = createDLedgerStore(dLedgerConfig.getStoreType(), this.dLedgerConfig, this.memberState);
        this.dLedgerRpcService = new DLedgerRpcNettyService(this, nettyServerConfig, nettyClientConfig, channelEventListener);
        this.rpcServiceMode = RpcServiceMode.EXCLUSIVE;
        this.dLedgerEntryPusher = new DLedgerEntryPusher(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);
        this.dLedgerLeaderElector = new DLedgerLeaderElector(dLedgerConfig, memberState, dLedgerRpcService);
        this.executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory(null, "DLedgerServer-ScheduledExecutor", true));
        this.fsmCaller = Optional.empty();
    }

    /**
     * Start in proxy mode, use shared DLedgerRpcService
     *
     * @param dLedgerConfig     DLedgerConfig
     * @param dLedgerRpcService Shared DLedgerRpcService
     */
    public DLedgerServer(DLedgerConfig dLedgerConfig, DLedgerRpcService dLedgerRpcService) {
        this.dLedgerConfig = dLedgerConfig;
        this.memberState = new MemberState(dLedgerConfig);
        this.dLedgerStore = createDLedgerStore(dLedgerConfig.getStoreType(), this.dLedgerConfig, this.memberState);
        this.dLedgerRpcService = dLedgerRpcService;
        this.rpcServiceMode = RpcServiceMode.SHARED;
        this.dLedgerLeaderElector = new DLedgerLeaderElector(dLedgerConfig, memberState, dLedgerRpcService);
        this.dLedgerEntryPusher = new DLedgerEntryPusher(dLedgerConfig, memberState, dLedgerStore, dLedgerRpcService);
        this.executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("DLedgerServer-ScheduledExecutor");
            return t;
        });
        this.fsmCaller = Optional.empty();
    }

    /**
     * Start up, if the DLedgerRpcService is exclusive for this DLedgerServer, we should also start up it.
     */
    public synchronized void startup() {
        if (!isStarted) {
            this.dLedgerStore.startup();
            this.fsmCaller.ifPresent(x -> {
                // Start state machine caller and load existing snapshots for data recovery
                x.start();
                x.getSnapshotManager().loadSnapshot();
            });
            if (RpcServiceMode.EXCLUSIVE.equals(this.rpcServiceMode)) {
                this.dLedgerRpcService.startup();
            }
            if (!memberState.isLearner()) {
                this.dLedgerLeaderElector.startup();
            }
            this.dLedgerEntryPusher.startup();
            executorService.scheduleAtFixedRate(this::checkPreferredLeader, 1000, 1000, TimeUnit.MILLISECONDS);
            isStarted = true;
        }
    }

    /**
     * Shutdown, if the DLedgerRpcService is exclusive for this DLedgerServer, we should also shut down it.
     */
    public synchronized void shutdown() {
        if (isStarted) {
            this.dLedgerLeaderElector.shutdown();
            this.dLedgerEntryPusher.shutdown();
            if (RpcServiceMode.EXCLUSIVE.equals(this.rpcServiceMode)) {
                this.dLedgerRpcService.shutdown();
            }
            this.dLedgerStore.shutdown();
            executorService.shutdown();
            this.fsmCaller.ifPresent(StateMachineCaller::shutdown);
            isStarted = false;
        }
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

    public synchronized void registerStateMachine(final StateMachine fsm) {
        if (isStarted) {
            throw new IllegalStateException("can not register statemachine after dledger server starts");
        }
        final StateMachineCaller fsmCaller = new StateMachineCaller(this.dLedgerStore, fsm, this.dLedgerEntryPusher);
        fsmCaller.registerSnapshotManager(new SnapshotManager(this));
        this.fsmCaller = Optional.of(fsmCaller);
        // Register state machine caller to entry pusher
        this.dLedgerEntryPusher.registerStateMachine(this.fsmCaller);
        if (dLedgerStore instanceof DLedgerMmapFileStore) {
            ((DLedgerMmapFileStore) dLedgerStore).setEnableCleanSpaceService(false);
        }
    }

    public StateMachine getStateMachine() {
        return this.fsmCaller.map(StateMachineCaller::getStateMachine).orElse(null);
    }

    @Override
    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        try {

            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleHeartBeat(request);
        } catch (DLedgerException e) {
            LOGGER.error("[{}][HandleHeartBeat] failed", memberState.getSelfId(), e);
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
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerLeaderElector.handleVote(request, false);
        } catch (DLedgerException e) {
            LOGGER.error("[{}][HandleVote] failed", memberState.getSelfId(), e);
            VoteResponse response = new VoteResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }
    }

    /**
     * Handle the append requests: 1.append the entry to local store 2.submit the future to entry pusher and wait the
     * quorum ack 3.if the pending requests are full, then reject it immediately
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
                        for (byte[] bytes : batchRequest.getBatchMsgs()) {
                            DLedgerEntry dLedgerEntry = new DLedgerEntry();
                            dLedgerEntry.setBody(bytes);
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
            LOGGER.error("[{}][HandleAppend] failed", memberState.getSelfId(), e);
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
            LOGGER.error("[{}][HandleGet] failed", memberState.getSelfId(), e);
            GetEntriesResponse response = new GetEntriesResponse();
            response.copyBaseInfo(request);
            response.setLeaderId(memberState.getLeaderId());
            response.setCode(e.getCode().getCode());
            return CompletableFuture.completedFuture(response);
        }
    }

    @Override
    public CompletableFuture<MetadataResponse> handleMetadata(MetadataRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            MetadataResponse metadataResponse = new MetadataResponse();
            metadataResponse.setGroup(memberState.getGroup());
            metadataResponse.setPeers(memberState.getPeerMap());
            metadataResponse.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(metadataResponse);
        } catch (DLedgerException e) {
            LOGGER.error("[{}][HandleMetadata] failed", memberState.getSelfId(), e);
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

    @Override
    public CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            return dLedgerEntryPusher.handlePush(request);
        } catch (DLedgerException e) {
            LOGGER.error("[{}][HandlePush] failed", memberState.getSelfId(), e);
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

                    LOGGER.warn("transferee fall behind, diff = {}", fallBehind);
                    Thread.sleep(10);

                    fallBehind = request.getTakeLeadershipLedgerIndex() - memberState.getLedgerEndIndex();
                    costTime = System.currentTimeMillis() - startTime;
                }

                return dLedgerLeaderElector.handleTakeLeadership(request);
            } else {
                return CompletableFuture.completedFuture(new LeadershipTransferResponse().term(memberState.currTerm()).code(DLedgerResponseCode.UNEXPECTED_ARGUMENT.getCode()));
            }
        } catch (DLedgerException e) {
            LOGGER.error("[{}][handleLeadershipTransfer] failed", memberState.getSelfId(), e);
            LeadershipTransferResponse response = new LeadershipTransferResponse();
            response.copyBaseInfo(request);
            response.setCode(e.getCode().getCode());
            response.setLeaderId(memberState.getLeaderId());
            return CompletableFuture.completedFuture(response);
        }

    }

    @Override
    public CompletableFuture<ChangePeersResponse> handleChangePeers(ChangePeersRequest request) throws Exception {
        try {
            PreConditions.check(memberState.getSelfId().equals(request.getRemoteId()), DLedgerResponseCode.UNKNOWN_MEMBER, "%s != %s", request.getRemoteId(), memberState.getSelfId());
            PreConditions.check(memberState.getGroup().equals(request.getGroup()), DLedgerResponseCode.UNKNOWN_GROUP, "%s != %s", request.getGroup(), memberState.getGroup());
            PreConditions.check(memberState.isLeader(), DLedgerResponseCode.NOT_LEADER, "node role not leader, node state != %s", memberState.getRole());
            PreConditions.check((request.getAddPeers().size() + request.getRemovePeers().size()) == 0, DLedgerResponseCode.PEERS_SIZE, "change peers failed, addPeer size: %d, remove size: %d", request.getAddPeers().size(), request.getRemovePeers().size());
            PreConditions.check(checkChangePeersGroup(request.getAddPeers()) && checkChangePeersGroup(request.getRemovePeers()), DLedgerResponseCode.UNKNOWN_GROUP, "request peers group != %s", memberState.getGroup());
            long currTerm = memberState.currTerm();

            if (dLedgerEntryPusher.isPendingFull(currTerm)) {
                ChangePeersResponse changePeersResponse = new ChangePeersResponse();
                changePeersResponse.setGroup(memberState.getGroup());
                changePeersResponse.setCode(DLedgerResponseCode.LEADER_PENDING_FULL.getCode());
                changePeersResponse.setTerm(currTerm);
                changePeersResponse.setLeaderId(memberState.getSelfId());
                return CompletableFuture.completedFuture(changePeersResponse);
            }
            DLedgerEntry dLedgerEntry = new DLedgerEntry();
            JSONObject awaitSyncPeers = new JSONObject();
            awaitSyncPeers.put("addPeers", request.getAddPeers());
            awaitSyncPeers.put("removePeers", request.getRemovePeers());
            dLedgerEntry.setBody(awaitSyncPeers.toJSONString().getBytes(StandardCharsets.UTF_8));
            DLedgerEntry resEntry = dLedgerStore.appendAsLeader(dLedgerEntry);
            return dLedgerEntryPusher.notifyChangePeers(request, resEntry);
        } catch (DLedgerException e) {
            return null;
        }
    }

    private boolean checkChangePeersGroup(List<String> peers) {
        String group = memberState.getGroup();
        for (String peer : peers) {
            if (!peer.split("-")[0].equals(group)) {
                return false;
            }
        }
        return true;
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
                LOGGER.warn("preferredLeaderId = {} is not a peer member", preferredLeaderId);
                continue;
            }

            if (!memberState.getPeersLiveTable().containsKey(preferredLeaderId) || !memberState.getPeersLiveTable().get(preferredLeaderId)) {
                it.remove();
                LOGGER.warn("preferredLeaderId = {} is not online", preferredLeaderId);
                continue;
            }

            if (dLedgerConfig.getLearnerAddressMap().containsKey(DLedgerUtils.generateDLedgerId(memberState.getGroup(), memberState.getSelfId()))) {
                continue;
            }
            long fallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(memberState.currTerm(), preferredLeaderId);
            if (fallBehind >= dLedgerConfig.getMaxLeadershipTransferWaitIndex()) {
                LOGGER.warn("preferredLeaderId = {} transferee fall behind index : {}", preferredLeaderId, fallBehind);
                continue;
            }
        }

        if (preferredLeaderIds.size() == 0) {
            return;
        }
        long minFallBehind = Long.MAX_VALUE;
        String preferredLeaderId = preferredLeaderIds.get(0);
        for (String peerId : preferredLeaderIds) {
            if (dLedgerConfig.getLearnerAddressMap().containsKey(DLedgerUtils.generateDLedgerId(memberState.getGroup(), peerId))) {
                continue;
            }
            long fallBehind = dLedgerStore.getLedgerEndIndex() - dLedgerEntryPusher.getPeerWaterMark(memberState.currTerm(), peerId);
            if (fallBehind < minFallBehind) {
                minFallBehind = fallBehind;
                preferredLeaderId = peerId;
            }
        }
        LOGGER.info("preferredLeaderId = {}, which has the smallest fall behind index = {} and is decided to be transferee.", preferredLeaderId, minFallBehind);

        if (minFallBehind < dLedgerConfig.getMaxLeadershipTransferWaitIndex()) {
            LeadershipTransferRequest request = new LeadershipTransferRequest();
            request.setTerm(memberState.currTerm());
            request.setTransfereeId(preferredLeaderId);

            try {
                long startTransferTime = System.currentTimeMillis();
                LeadershipTransferResponse response = dLedgerLeaderElector.handleLeadershipTransfer(request).get();
                LOGGER.info("transfer finished. request={},response={},cost={}ms", request, response, DLedgerUtils.elapsed(startTransferTime));
            } catch (Throwable t) {
                LOGGER.error("[checkPreferredLeader] error, request={}", request, t);
            }
        }
    }

    @Deprecated
    public DLedgerStore getdLedgerStore() {
        return dLedgerStore;
    }

    public DLedgerStore getDLedgerStore() {
        return dLedgerStore;
    }

    @Deprecated
    public DLedgerRpcService getdLedgerRpcService() {
        return dLedgerRpcService;
    }

    public DLedgerRpcService getDLedgerRpcService() {
        return dLedgerRpcService;
    }

    @Deprecated
    public DLedgerLeaderElector getdLedgerLeaderElector() {
        return dLedgerLeaderElector;
    }

    public DLedgerLeaderElector getDLedgerLeaderElector() {
        return dLedgerLeaderElector;
    }

    @Deprecated
    public DLedgerConfig getdLedgerConfig() {
        return dLedgerConfig;
    }

    @Override
    public String getListenAddress() {
        return this.dLedgerConfig.getSelfAddress();
    }

    @Override
    public String getPeerAddr(String groupId, String selfId) {
        String address = this.dLedgerConfig.getPeerAddressMap().get(DLedgerUtils.generateDLedgerId(groupId, selfId));
        if (address == null) {
            address = this.dLedgerConfig.getLearnerAddressMap().get(DLedgerUtils.generateDLedgerId(groupId, selfId));
        }
        return address;
    }

    public DLedgerConfig getDLedgerConfig() {
        return dLedgerConfig;
    }

    @Override
    public NettyRemotingServer getRemotingServer() {
        if (this.dLedgerRpcService instanceof DLedgerRpcNettyService) {
            return ((DLedgerRpcNettyService) this.dLedgerRpcService).getRemotingServer();
        }
        return null;
    }

    @Override
    public NettyRemotingClient getRemotingClient() {
        if (this.dLedgerRpcService instanceof DLedgerRpcNettyService) {
            return ((DLedgerRpcNettyService) this.dLedgerRpcService).getRemotingClient();
        }
        return null;
    }

    public StateMachineCaller getFsmCaller() {
        return fsmCaller.orElseThrow(NullPointerException::new);
    }

    public boolean isLeader() {
        return this.memberState.isLeader();
    }

    /**
     * Rpc service mode, exclusive or shared
     */
    enum RpcServiceMode {
        EXCLUSIVE,
        SHARED
    }

}
