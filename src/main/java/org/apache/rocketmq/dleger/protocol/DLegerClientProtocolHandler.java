package org.apache.rocketmq.dleger.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Both the RaftLogServer(inbound) and RaftRpcService (outbound) should implement this protocol
 */
public interface DLegerClientProtocolHandler {


    CompletableFuture<AppendEntryResponse> handleAppend(AppendEntryRequest request) throws Exception;
    CompletableFuture<GetEntriesResponse> handleGet(GetEntriesRequest request) throws Exception;

}
