package org.apache.rocketmq.dleger.protocol;

import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.MemberState;
import org.apache.rocketmq.dleger.ServerTestBase;
import org.apache.rocketmq.dleger.client.DLegerClient;
import org.apache.rocketmq.dleger.util.FileTestUtil;

public class ServerTestHarness extends ServerTestBase {

    protected synchronized DLegerServer launchServer(String group, String peers, String selfId) {
        DLegerConfig config = new DLegerConfig();
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreType(DLegerConfig.MEMORY);
        DLegerServer dLegerServer = new DLegerServer(config);
        dLegerServer.startup();
        bases.add(config.getDefaultPath());
        return dLegerServer;
    }


    protected synchronized DLegerServer launchServer(String group, String peers, String selfId, String leaderId, String storeType) {
        DLegerConfig config = new DLegerConfig();
        config.group(group).selfId(selfId).peers(peers);
        config.setStoreBaseDir(FileTestUtil.TEST_BASE);
        config.setStoreType(storeType);
        config.setEnableLeaderElector(false);
        DLegerServer dLegerServer = new DLegerServer(config);
        MemberState memberState = dLegerServer.getMemberState();
        memberState.setCurrTerm(0);
        if (selfId.equals(leaderId)) {
            memberState.changeToLeader(0);
        } else {
            memberState.changeToFollower(0, leaderId);
        }
        bases.add(config.getDataStorePath());
        bases.add(config.getIndexStorePath());
        bases.add(config.getDefaultPath());
        dLegerServer.startup();
        return dLegerServer;
    }

    protected synchronized DLegerClient launchClient(String group, String peers) {
        DLegerClient dLegerClient = new DLegerClient(peers);
        dLegerClient.startup();
        return dLegerClient;
    }
}
