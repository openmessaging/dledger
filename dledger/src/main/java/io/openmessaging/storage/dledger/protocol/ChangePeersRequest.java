package io.openmessaging.storage.dledger.protocol;/* 
    create qiangzhiwei time 2022/11/6
 */

import java.util.ArrayList;
import java.util.List;

public class ChangePeersRequest extends RequestOrResponse {
    private List<String> addPeers = new ArrayList<>();
    private List<String> removePeers = new ArrayList<>();

    public void setAddPeers(List<String> addPeers) {
        this.addPeers = addPeers;
    }

    public void setRemovePeers(List<String> removePeers) {
        this.removePeers = removePeers;
    }

    public List<String> getAddPeers() {
        return addPeers;
    }

    public List<String> getRemovePeers() {
        return removePeers;
    }
}
