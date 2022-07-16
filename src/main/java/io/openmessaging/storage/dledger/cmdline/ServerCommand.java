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

package io.openmessaging.storage.dledger.cmdline;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.openmessaging.storage.dledger.DLedger;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.utils.IOUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "start dledger server")
public class ServerCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(ServerCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--id", "-i"}, description = "Self id of this server")
    private String selfId = "n0";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--store-base-dir", "-s"}, description = "The base store dir of this server")
    private String storeBaseDir = File.separator + "tmp" + File.separator + "dledgerstore";

    @Parameter(names = {"--read-only-data-store-dirs"}, description = "The dirs of this server to be read only")
    private String readOnlyDataStoreDirs;

    @Parameter(names = {"--peer-push-throttle-point"}, description = "When the follower is behind the leader more than this value, it will trigger the throttle")
    private int peerPushThrottlePoint = 300 * 1024 * 1024;

    @Parameter(names = {"--peer-push-quotas"}, description = "The quotas of the pusher")
    private int peerPushQuota = 20 * 1024 * 1024;

    @Parameter(names = {"--preferred-leader-id"}, description = "Preferred LeaderId")
    private String preferredLeaderIds;

    @Parameter(names = {"--config-file", "-c"}, description = "Dledger config properties file")
    private String configFile;

    @Override
    public void doCommand() {

        DLedgerConfig dLedgerConfig = new DLedgerConfig();
        mergeCommandAttributeIntoConfig(dLedgerConfig);
        mergePropertiesIntoConfig(dLedgerConfig);
        DLedger.startup(dLedgerConfig);
        logger.info("[{}] Dledger Server started", selfId);
    }

    private void mergePropertiesIntoConfig(DLedgerConfig dLedgerConfig) {

        if (null == configFile || configFile.trim().isEmpty()) {
            return;
        }
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configFile));
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
        IOUtils.properties2Object(properties, dLedgerConfig);
    }

    private void mergeCommandAttributeIntoConfig(DLedgerConfig dLedgerConfig) {
        Field[] fields = ServerCommand.this.getClass().getDeclaredFields();
        if (fields == null || fields.length == 0) {
            return;
        }
        Properties properties = new Properties();
        for (Field field : fields) {
            if (field.getType() != String.class) {
                continue;
            }
            field.setAccessible(true);
            try {
                Object value = field.get(ServerCommand.this);
                if (value == null) {
                    continue;
                }
                properties.put(field.getName(), value);
            } catch (IllegalAccessException e) {
                logger.warn("get field value error", e);
            }
        }
        IOUtils.properties2Object(properties, dLedgerConfig);
    }
}
