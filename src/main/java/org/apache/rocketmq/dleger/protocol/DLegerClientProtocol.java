package org.apache.rocketmq.dleger.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Both the RaftLogServer(inbound) and RaftRpcService (outbound) should implement this protocol
 */
public interface DLegerClientProtocol {


    CompletableFuture<GetEntriesResponse> get(GetEntriesRequest request) throws Exception;
    CompletableFuture<AppendEntryResponse> append(AppendEntryRequest request) throws Exception;
    CompletableFuture<MetadataResponse> metadata(MetadataRequest request) throws Exception;

}
