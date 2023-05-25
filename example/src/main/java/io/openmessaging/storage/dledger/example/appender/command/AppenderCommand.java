/*
 * Copyright 2017-2022 The DLedger Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.example.appender.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.example.appender.AppenderDLedger;
import io.openmessaging.storage.dledger.example.common.command.BaseCommand;
import io.openmessaging.storage.dledger.proxy.DLedgerProxyConfig;
import io.openmessaging.storage.dledger.proxy.util.ConfigUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "Bootstrap the AppenderDLedger")
public class AppenderCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(AppenderCommand.class);

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

    @Override
    public void doCommand() {
        try {
            List<DLedgerConfig> dLedgerConfigs = buildDLedgerConfigs();
            AppenderDLedger.bootstrapDLedger(dLedgerConfigs);
            logger.info("Bootstrap DLedger servers success, configs = {}", dLedgerConfigs);
        } catch (Exception e) {
            logger.error("Bootstrap DLedger servers error", e);
        }
    }

    private List<DLedgerConfig> buildDLedgerConfigs() throws Exception {
        List<DLedgerConfig> dLedgerConfigs = new ArrayList<>();
        if (this.configFile == null) {
            DLedgerConfig dLedgerConfig = new DLedgerConfig();
            dLedgerConfig.setGroup(this.group);
            dLedgerConfig.setSelfId(this.selfId);
            dLedgerConfig.setPeers(this.peers);
            dLedgerConfig.setStoreBaseDir(this.storeBaseDir);
            dLedgerConfig.setReadOnlyDataStoreDirs(this.readOnlyDataStoreDirs);
            dLedgerConfig.setPeerPushThrottlePoint(this.peerPushThrottlePoint);
            dLedgerConfig.setPeerPushQuota(this.peerPushQuota);
            dLedgerConfig.setPreferredLeaderIds(this.preferredLeaderIds);
            dLedgerConfigs.add(dLedgerConfig);
        } else {
            DLedgerProxyConfig config = ConfigUtils.parseDLedgerProxyConfig(configFile);
            dLedgerConfigs = config.getConfigs();
        }
        return dLedgerConfigs;
    }
}
