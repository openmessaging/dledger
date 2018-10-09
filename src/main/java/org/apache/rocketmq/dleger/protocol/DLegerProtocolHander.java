package org.apache.rocketmq.dleger.protocol;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Both the RaftLogServer(inbound) and RaftRpcService (outbound) should implement this protocol
 */
public interface DLegerProtocolHander extends DLegerClientProtocolHandler {


    CompletableFuture<VoteResponse> handleVote(VoteRequest request) throws Exception;

    CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception;

    CompletableFuture<PullEntriesResponse> handlePull(PullEntriesRequest request) throws Exception;

    CompletableFuture<PushEntryResponse> handlePush(PushEntryRequest request) throws Exception;

}
