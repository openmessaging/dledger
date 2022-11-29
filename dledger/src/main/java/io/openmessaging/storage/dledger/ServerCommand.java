/*
 * Copyright 2017-2022 The DLedger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.io.File;

@Parameters(commandDescription = "DLedger server config")
public class ServerCommand {

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--id", "-i"}, description = "Self id of this server")
    private String selfId = "n0";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--store-base-dir", "-s"}, description = "The base store dir of this server")
    private String storeBaseDir = File.separator + "tmp" + File.separator + "dledgerstore";

    @Parameter(names = {"--read-only-data-store-dirs"}, description = "The dirs of this server to be read only")
    private String readOnlyDataStoreDirs = null;

    @Parameter(names = {"--peer-push-throttle-point"}, description = "When the follower is behind the leader more than this value, it will trigger the throttle")
    private int peerPushThrottlePoint = 300 * 1024 * 1024;

    @Parameter(names = {"--peer-push-quotas"}, description = "The quotas of the pusher")
    private int peerPushQuota = 20 * 1024 * 1024;

    @Parameter(names = {"--preferred-leader-id"}, description = "Preferred LeaderId")
    private String preferredLeaderIds = null;

    @Parameter(names = {"--config-file", "-c"}, description = "DLedger config properties file")
    private String configFile = null;

    @Parameter(names = {"--help", "-h"}, help = true, description = "usage print")
    private boolean help;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getSelfId() {
        return selfId;
    }

    public void setSelfId(String selfId) {
        this.selfId = selfId;
    }

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    public String getStoreBaseDir() {
        return storeBaseDir;
    }

    public void setStoreBaseDir(String storeBaseDir) {
        this.storeBaseDir = storeBaseDir;
    }

    public String getReadOnlyDataStoreDirs() {
        return readOnlyDataStoreDirs;
    }

    public void setReadOnlyDataStoreDirs(String readOnlyDataStoreDirs) {
        this.readOnlyDataStoreDirs = readOnlyDataStoreDirs;
    }

    public int getPeerPushThrottlePoint() {
        return peerPushThrottlePoint;
    }

    public void setPeerPushThrottlePoint(int peerPushThrottlePoint) {
        this.peerPushThrottlePoint = peerPushThrottlePoint;
    }

    public int getPeerPushQuota() {
        return peerPushQuota;
    }

    public void setPeerPushQuota(int peerPushQuota) {
        this.peerPushQuota = peerPushQuota;
    }

    public String getPreferredLeaderIds() {
        return preferredLeaderIds;
    }

    public void setPreferredLeaderIds(String preferredLeaderIds) {
        this.preferredLeaderIds = preferredLeaderIds;
    }

    public String getConfigFile() {
        return configFile;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }
}
