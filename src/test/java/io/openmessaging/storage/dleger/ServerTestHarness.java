/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dleger;

import io.openmessaging.storage.dleger.client.DLegerClient;
import io.openmessaging.storage.dleger.util.FileTestUtil;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

    protected synchronized DLegerServer launchServer(String group, String peers, String selfId, String leaderId,
        String storeType) {
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
        DLegerClient dLegerClient = new DLegerClient(group, peers);
        dLegerClient.startup();
        return dLegerClient;
    }

    protected DLegerServer parseServers(List<DLegerServer> servers, AtomicInteger leaderNum,
        AtomicInteger followerNum) {
        DLegerServer leaderServer = null;
        for (DLegerServer server : servers) {
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
