/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.snapshot.SnapshotEntryResetStrategy;
import io.openmessaging.storage.dledger.store.file.DLedgerMmapFileStore;

import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class DLedgerConfig {

    public static final String MEMORY = "MEMORY";
    public static final String FILE = "FILE";
    public static final String MULTI_PATH_SPLITTER = System.getProperty("dLedger.multiPath.Splitter", ",");

    private String configFilePath;

    private String group = "default";

    private String selfId = "n0";

    private String peers = "n0-localhost:20911";

    private String storeBaseDir = File.separator + "tmp" + File.separator + "dledgerstore";

    private String readOnlyDataStoreDirs = null;

    private int peerPushThrottlePoint = 300 * 1024 * 1024;

    private int peerPushQuota = 20 * 1024 * 1024;

    private String storeType = FILE; //FILE, MEMORY
    private String dataStorePath;

    private int maxPendingRequestsNum = 10000;

    private int maxWaitAckTimeMs = 2500;

    private int maxPushTimeOutMs = 1000;

    private boolean enableLeaderElector = true;

    private int heartBeatTimeIntervalMs = 2000;

    private int maxHeartBeatLeak = 3;

    private int minVoteIntervalMs = 300;
    private int maxVoteIntervalMs = 1000;

    private int fileReservedHours = 72;
    private String deleteWhen = "04";

    private float diskSpaceRatioToCheckExpired = Float.parseFloat(System.getProperty("dledger.disk.ratio.check", "0.70"));
    private float diskSpaceRatioToForceClean = Float.parseFloat(System.getProperty("dledger.disk.ratio.clean", "0.85"));

    private boolean enableDiskForceClean = true;

    private long flushFileInterval = 10;

    private long checkPointInterval = 3000;

    private int mappedFileSizeForEntryData = 1024 * 1024 * 1024;
    private int mappedFileSizeForEntryIndex = DLedgerMmapFileStore.INDEX_UNIT_SIZE * 5 * 1024 * 1024;

    private boolean enablePushToFollower = true;

    private String preferredLeaderIds;
    private long maxLeadershipTransferWaitIndex = 1000;
    private int minTakeLeadershipVoteIntervalMs = 30;
    private int maxTakeLeadershipVoteIntervalMs = 100;

    private boolean isEnableBatchAppend = false;

    // max size in bytes for each append request
    private int maxBatchAppendSize = 4 * 1024;

    private long leadershipTransferWaitTimeout = 1000;

    private boolean enableSnapshot = false;

    private SnapshotEntryResetStrategy snapshotEntryResetStrategy = SnapshotEntryResetStrategy.RESET_ALL_SYNC;

    private int snapshotThreshold = 1000;

    private int resetSnapshotEntriesDelayTime = 5 * 1000;

    /**
     * reset snapshot entries but keep last entries num.
     * .e.g 10, when we load from snapshot which lastIncludedIndex = 100, we will delete the entries in (..., 90]
     */
    private int resetSnapshotEntriesButKeepLastEntriesNum = 10;
    private int maxSnapshotReservedNum = 3;

    // max interval in ms for each append request
    private int maxBatchAppendIntervalMs = 1000;

    /**
     * When node change from candidate to leader, it maybe always keep the old commit index although this index's entry has been
     * replicated to more than half of the nodes (it will keep until a new entry is appended in current term).
     * The reason why this scenario happens is that leader can't commit the entries which are belong to the previous term.
     */
    private boolean enableFastAdvanceCommitIndex = false;

    public String getDefaultPath() {
        return storeBaseDir + File.separator + "dledger-" + selfId;
    }

    public String getDataStorePath() {
        if (dataStorePath == null) {
            return getDefaultPath() + File.separator + "data";
        }
        return dataStorePath;
    }

    public void setDataStorePath(String dataStorePath) {
        this.dataStorePath = dataStorePath;
    }

    public String getSnapshotStoreBaseDir() {
        return getDefaultPath() + File.separator + "snapshot";
    }

    public String getIndexStorePath() {
        return getDefaultPath() + File.separator + "index";
    }

    public int getMappedFileSizeForEntryData() {
        return mappedFileSizeForEntryData;
    }

    public void setMappedFileSizeForEntryData(int mappedFileSizeForEntryData) {
        this.mappedFileSizeForEntryData = mappedFileSizeForEntryData;
    }

    public int getMappedFileSizeForEntryIndex() {
        return mappedFileSizeForEntryIndex;
    }

    public void setMappedFileSizeForEntryIndex(int mappedFileSizeForEntryIndex) {
        this.mappedFileSizeForEntryIndex = mappedFileSizeForEntryIndex;
    }

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

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public boolean isEnableLeaderElector() {
        return enableLeaderElector;
    }

    public void setEnableLeaderElector(boolean enableLeaderElector) {
        this.enableLeaderElector = enableLeaderElector;
    }

    //for builder semantic
    public DLedgerConfig group(String group) {
        this.group = group;
        return this;
    }

    public DLedgerConfig selfId(String selfId) {
        this.selfId = selfId;
        return this;
    }

    public DLedgerConfig peers(String peers) {
        this.peers = peers;
        return this;
    }

    public DLedgerConfig storeBaseDir(String dir) {
        this.storeBaseDir = dir;
        return this;
    }

    public boolean isEnablePushToFollower() {
        return enablePushToFollower;
    }

    public void setEnablePushToFollower(boolean enablePushToFollower) {
        this.enablePushToFollower = enablePushToFollower;
    }

    public int getMaxPendingRequestsNum() {
        return maxPendingRequestsNum;
    }

    public void setMaxPendingRequestsNum(int maxPendingRequestsNum) {
        this.maxPendingRequestsNum = maxPendingRequestsNum;
    }

    public int getMaxWaitAckTimeMs() {
        return maxWaitAckTimeMs;
    }

    public void setMaxWaitAckTimeMs(int maxWaitAckTimeMs) {
        this.maxWaitAckTimeMs = maxWaitAckTimeMs;
    }

    public int getMaxPushTimeOutMs() {
        return maxPushTimeOutMs;
    }

    public void setMaxPushTimeOutMs(int maxPushTimeOutMs) {
        this.maxPushTimeOutMs = maxPushTimeOutMs;
    }

    public int getHeartBeatTimeIntervalMs() {
        return heartBeatTimeIntervalMs;
    }

    public void setHeartBeatTimeIntervalMs(int heartBeatTimeIntervalMs) {
        this.heartBeatTimeIntervalMs = heartBeatTimeIntervalMs;
    }

    public int getMinVoteIntervalMs() {
        return minVoteIntervalMs;
    }

    public void setMinVoteIntervalMs(int minVoteIntervalMs) {
        this.minVoteIntervalMs = minVoteIntervalMs;
    }

    public int getMaxVoteIntervalMs() {
        return maxVoteIntervalMs;
    }

    public void setMaxVoteIntervalMs(int maxVoteIntervalMs) {
        this.maxVoteIntervalMs = maxVoteIntervalMs;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public float getDiskSpaceRatioToCheckExpired() {
        return diskSpaceRatioToCheckExpired;
    }

    public void setDiskSpaceRatioToCheckExpired(float diskSpaceRatioToCheckExpired) {
        this.diskSpaceRatioToCheckExpired = diskSpaceRatioToCheckExpired;
    }

    public float getDiskSpaceRatioToForceClean() {
        if (diskSpaceRatioToForceClean < 0.50f) {
            return 0.50f;
        } else {
            return diskSpaceRatioToForceClean;
        }
    }

    public void setDiskSpaceRatioToForceClean(float diskSpaceRatioToForceClean) {
        this.diskSpaceRatioToForceClean = diskSpaceRatioToForceClean;
    }

    public float getDiskFullRatio() {
        float ratio = diskSpaceRatioToForceClean + 0.05f;
        if (ratio > 0.95f) {
            return 0.95f;
        }
        return ratio;
    }

    public int getFileReservedHours() {
        return fileReservedHours;
    }

    public void setFileReservedHours(int fileReservedHours) {
        this.fileReservedHours = fileReservedHours;
    }

    public long getFlushFileInterval() {
        return flushFileInterval;
    }

    public void setFlushFileInterval(long flushFileInterval) {
        this.flushFileInterval = flushFileInterval;
    }

    public boolean isEnableDiskForceClean() {
        return enableDiskForceClean;
    }

    public void setEnableDiskForceClean(boolean enableDiskForceClean) {
        this.enableDiskForceClean = enableDiskForceClean;
    }

    public int getMaxHeartBeatLeak() {
        return maxHeartBeatLeak;
    }

    public void setMaxHeartBeatLeak(int maxHeartBeatLeak) {
        this.maxHeartBeatLeak = maxHeartBeatLeak;
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

    public long getCheckPointInterval() {
        return checkPointInterval;
    }

    public void setCheckPointInterval(long checkPointInterval) {
        this.checkPointInterval = checkPointInterval;
    }

    @Deprecated
    public String getPreferredLeaderId() {
        return preferredLeaderIds;
    }

    @Deprecated
    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderIds = preferredLeaderId;
    }

    public String getPreferredLeaderIds() {
        return preferredLeaderIds;
    }

    public void setPreferredLeaderIds(String preferredLeaderIds) {
        this.preferredLeaderIds = preferredLeaderIds;
    }

    public long getMaxLeadershipTransferWaitIndex() {
        return maxLeadershipTransferWaitIndex;
    }

    public void setMaxLeadershipTransferWaitIndex(long maxLeadershipTransferWaitIndex) {
        this.maxLeadershipTransferWaitIndex = maxLeadershipTransferWaitIndex;
    }

    public int getMinTakeLeadershipVoteIntervalMs() {
        return minTakeLeadershipVoteIntervalMs;
    }

    public void setMinTakeLeadershipVoteIntervalMs(int minTakeLeadershipVoteIntervalMs) {
        this.minTakeLeadershipVoteIntervalMs = minTakeLeadershipVoteIntervalMs;
    }

    public int getMaxTakeLeadershipVoteIntervalMs() {
        return maxTakeLeadershipVoteIntervalMs;
    }

    public void setMaxTakeLeadershipVoteIntervalMs(int maxTakeLeadershipVoteIntervalMs) {
        this.maxTakeLeadershipVoteIntervalMs = maxTakeLeadershipVoteIntervalMs;
    }

    public boolean isEnableBatchAppend() {
        return isEnableBatchAppend;
    }

    public void setEnableBatchAppend(boolean enableBatchAppend) {
        isEnableBatchAppend = enableBatchAppend;
    }

    public int getMaxBatchAppendSize() {
        return maxBatchAppendSize;
    }

    public void setMaxBatchAppendSize(int maxBatchAppendSize) {
        this.maxBatchAppendSize = maxBatchAppendSize;
    }

    public long getLeadershipTransferWaitTimeout() {
        return leadershipTransferWaitTimeout;
    }

    public void setLeadershipTransferWaitTimeout(long leadershipTransferWaitTimeout) {
        this.leadershipTransferWaitTimeout = leadershipTransferWaitTimeout;
    }

    public String getReadOnlyDataStoreDirs() {
        return readOnlyDataStoreDirs;
    }

    public void setReadOnlyDataStoreDirs(String readOnlyDataStoreDirs) {
        this.readOnlyDataStoreDirs = readOnlyDataStoreDirs;
    }

    private String selfAddress;

    // groupId#selfIf -> address
    private Map<String, String> peerAddressMap;

    private final AtomicBoolean inited = new AtomicBoolean(false);

    public void init() {
        if (inited.compareAndSet(false, true)) {
            initSelfAddress();
            initPeerAddressMap();
        }
    }

    private void initSelfAddress() {
        for (String peerInfo : this.peers.split(";")) {
            String peerSelfId = peerInfo.split("-")[0];
            String peerAddress = peerInfo.substring(peerSelfId.length() + 1);
            if (this.selfId.equals(peerSelfId)) {
                this.selfAddress = peerAddress;
                return;
            }
        }
        // can't find itself
        throw new IllegalArgumentException("[DLedgerConfig] fail to init self address, config: " + this);
    }

    private void initPeerAddressMap() {
        Map<String, String> peerMap = new HashMap<>();
        for (String peerInfo : this.peers.split(";")) {
            String peerSelfId = peerInfo.split("-")[0];
            String peerAddress = peerInfo.substring(peerSelfId.length() + 1);
            peerMap.put(DLedgerUtils.generateDLedgerId(this.group, peerSelfId), peerAddress);
        }
        this.peerAddressMap = peerMap;
    }

    public String getSelfAddress() {
        return this.selfAddress;
    }


    public Map<String, String> getPeerAddressMap() {
        return this.peerAddressMap;
    }

    public int getSnapshotThreshold() {
        return snapshotThreshold;
    }

    public void setSnapshotThreshold(int snapshotThreshold) {
        this.snapshotThreshold = snapshotThreshold;
    }

    public int getMaxSnapshotReservedNum() {
        return maxSnapshotReservedNum;
    }

    public void setMaxSnapshotReservedNum(int maxSnapshotReservedNum) {
        this.maxSnapshotReservedNum = maxSnapshotReservedNum;
    }

    public boolean isEnableSnapshot() {
        return enableSnapshot;
    }

    public void setEnableSnapshot(boolean enableSnapshot) {
        this.enableSnapshot = enableSnapshot;
    }

    public int getMaxBatchAppendIntervalMs() {
        return maxBatchAppendIntervalMs;
    }

    public SnapshotEntryResetStrategy getSnapshotEntryResetStrategy() {
        return snapshotEntryResetStrategy;
    }

    public void setSnapshotEntryResetStrategy(
        SnapshotEntryResetStrategy snapshotEntryResetStrategy) {
        this.snapshotEntryResetStrategy = snapshotEntryResetStrategy;
    }

    public void setMaxBatchAppendIntervalMs(int maxBatchAppendIntervalMs) {
        this.maxBatchAppendIntervalMs = maxBatchAppendIntervalMs;
    }

    public int getResetSnapshotEntriesDelayTime() {
        return resetSnapshotEntriesDelayTime;
    }

    public void setResetSnapshotEntriesDelayTime(int resetSnapshotEntriesDelayTime) {
        this.resetSnapshotEntriesDelayTime = resetSnapshotEntriesDelayTime;
    }

    public int getResetSnapshotEntriesButKeepLastEntriesNum() {
        return resetSnapshotEntriesButKeepLastEntriesNum;
    }

    public void setResetSnapshotEntriesButKeepLastEntriesNum(int resetSnapshotEntriesButKeepLastEntriesNum) {
        this.resetSnapshotEntriesButKeepLastEntriesNum = resetSnapshotEntriesButKeepLastEntriesNum;
    }

    public boolean isEnableFastAdvanceCommitIndex() {
        return enableFastAdvanceCommitIndex;
    }

    public void setEnableFastAdvanceCommitIndex(boolean enableFastAdvanceCommitIndex) {
        this.enableFastAdvanceCommitIndex = enableFastAdvanceCommitIndex;
    }
}
