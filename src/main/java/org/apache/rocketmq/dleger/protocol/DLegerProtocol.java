package org.apache.rocketmq.dleger.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Both the RaftLogServer(inbound) and RaftRpcService (outbound) should implement this protocol
 */
public interface DLegerProtocol extends DLegerClientProtocol {


    CompletableFuture<VoteResponse> vote(VoteRequest request) throws Exception;

    CompletableFuture<HeartBeatResponse> heartBeat(HeartBeatRequest request) throws Exception;

    CompletableFuture<PullEntriesResponse> pull(PullEntriesRequest request) throws Exception;

    CompletableFuture<PushEntryResponse> push(PushEntryRequest request) throws Exception;

}
