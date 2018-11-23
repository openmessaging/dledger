package io.openmessaging.storage.dleger.protocol;

import java.util.Map;

public class MetadataResponse extends RequestOrResponse {


    private Map<String, String> peers;

    public Map<String, String> getPeers() {
        return peers;
    }

    public void setPeers(Map<String, String> peers) {
        this.peers = peers;
    }
}
