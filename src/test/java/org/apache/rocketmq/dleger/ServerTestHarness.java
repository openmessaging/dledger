package org.apache.rocketmq.dleger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
        config.setMappedFileSizeForEntryData(10 * 1024 * 1024);
        config.setEnableLeaderElector(false);
        config.setEnableDiskForceClean(false);
        config.setDiskSpaceRatioToForceClean(0.90f);
        DLegerServer dLegerServer = new DLegerServer(config);
        MemberState memberState = dLegerServer.getMemberState();
        memberState.setCurrTermForTest(0);
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

    protected DLegerServer parseServers(List<DLegerServer> servers, AtomicInteger leaderNum, AtomicInteger followerNum) {
        DLegerServer leaderServer  = null;
        for (DLegerServer server: servers) {
            if (server.getMemberState().isLeader()) {
                leaderNum.incrementAndGet();
                leaderServer = server;
            } else if (server.getMemberState().isFollower()) {
                followerNum.incrementAndGet();
            }
        }
        return leaderServer;
    }
}
