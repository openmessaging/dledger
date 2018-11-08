package org.apache.rocketmq.dleger.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.dleger.protocol.DLegerClientProtocol;

public abstract class DLegerClientRpcService implements DLegerClientProtocol {
    private Map<String, String> peerMap = new ConcurrentHashMap<>();
    public void updatePeers(String peers) {
        for (String peerInfo: peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
    }

    public void updatePeers(Map<String, String> peers) {
        peerMap.putAll(peers);
    }

    public String getPeerAddr(String id) {
        return peerMap.get(id);
    }

    public abstract void startup();
    public abstract void shutdown();
}
