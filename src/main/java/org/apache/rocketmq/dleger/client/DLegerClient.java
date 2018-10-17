package org.apache.rocketmq.dleger.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.GetEntriesRequest;
import org.apache.rocketmq.dleger.protocol.GetEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerClient {

    private static Logger logger = LoggerFactory.getLogger(DLegerClient.class);
    private Map<String, String> peerMap = new ConcurrentHashMap<>();
    public void updatePeers(String peers) {
        for (String peerInfo: peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }


    private String leaderId;
    private DLegerClientRpcService dLegerClientRpcService;


    public DLegerClient(String peers) {
        updatePeers(peers);
        dLegerClientRpcService = new DLegerClientRpcNettyService();
        dLegerClientRpcService.updatePeers(peers);
        leaderId = peerMap.keySet().iterator().next();
    }


    public AppendEntryResponse append(byte[] body) {
        try {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setRemoteId(leaderId);
            appendEntryRequest.setBody(body);
            AppendEntryResponse response = dLegerClientRpcService.append(appendEntryRequest).get();
            if (response.getCode() == DLegerException.Code.NOT_LEADER.ordinal()) {
                leaderId = response.getLeaderId();
                appendEntryRequest.setRemoteId(leaderId);
                response = dLegerClientRpcService.append(appendEntryRequest).get();
            }
            return response;
        } catch (Exception e) {
            logger.error("{}", e);
            AppendEntryResponse appendEntryResponse = new AppendEntryResponse();
            appendEntryResponse.setCode(DLegerResponseCode.INTERNAL_ERROR);
            return appendEntryResponse;
        }
    }

    public GetEntriesResponse get(long index){
        try {
            GetEntriesRequest request = new GetEntriesRequest();
            request.setRemoteId(leaderId);
            request.setBeginIndex(index);
            GetEntriesResponse response = dLegerClientRpcService.get(request).get();
            if (response.getCode() == DLegerException.Code.NOT_LEADER.ordinal()) {
                leaderId = response.getLeaderId();
                request.setRemoteId(leaderId);
                response = dLegerClientRpcService.get(request).get();
            }
            return response;
        } catch (Exception e) {
            GetEntriesResponse getEntriesResponse =  new GetEntriesResponse();
            getEntriesResponse.setCode(DLegerResponseCode.INTERNAL_ERROR);
            return getEntriesResponse;
        }
    }



    public void startup() {
        this.dLegerClientRpcService.startup();
    }

    public void shutdown() {
        this.dLegerClientRpcService.shutdown();
    }
}
