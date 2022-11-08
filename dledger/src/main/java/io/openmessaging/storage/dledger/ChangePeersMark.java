package io.openmessaging.storage.dledger;/* 
    create qiangzhiwei time 2022/11/8
 */

import java.util.List;

public class ChangePeersMark {
    private long term;
    private long changePeersEntryIndex;
    private List<String> addPeers;
    private List<String> removePeers;

    public ChangePeersMark() {
    }

    public ChangePeersMark(long term,long changePeersEntryIndex, List<String> addPeers, List<String> removePeers) {
        this.term = term;
        this.changePeersEntryIndex = changePeersEntryIndex;
        this.addPeers = addPeers;
        this.removePeers = removePeers;
    }
}
