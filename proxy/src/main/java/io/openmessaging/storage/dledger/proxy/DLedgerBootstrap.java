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

package io.openmessaging.storage.dledger.proxy;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.core.DLedgerConfig;
import io.openmessaging.storage.dledger.proxy.util.ConfigUtils;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerBootstrap {

    private static Logger logger = LoggerFactory.getLogger(DLedgerBootstrap.class);

    public static void main(String[] args) {
        List<DLedgerConfig> dLedgerConfigs = new LinkedList<>();
        if (args.length > 0 && ("--config".equals(args[0]) || "-c".equals(args[0]))) {
            ConfigCommand configCommand = new ConfigCommand();
            JCommander.newBuilder().addObject(configCommand).build().parse(args);
            try {
                DLedgerProxyConfig dLedgerProxyConfig = ConfigUtils.parseDLedgerProxyConfig(configCommand.getConfigPath());
                dLedgerConfigs.addAll(dLedgerProxyConfig.getConfigs());
            } catch (Exception e) {
                logger.error("Create DLedgerProxyConfig error", e);
                System.exit(-1);
            }
        } else {
            DLedgerConfig dLedgerConfig = new DLedgerConfig();
            JCommander.newBuilder().addObject(dLedgerConfig).build().parse(args);
            dLedgerConfigs.add(dLedgerConfig);
        }
        bootstrapDLedger(dLedgerConfigs);
    }

    public static void bootstrapDLedger(List<DLedgerConfig> dLedgerConfigs) {
        if (dLedgerConfigs == null || dLedgerConfigs.isEmpty()) {
            logger.error("Bootstrap DLedger server error", new IllegalArgumentException("DLedgerConfigs is null or empty"));
        }
        DLedgerProxy dLedgerProxy = new DLedgerProxy(dLedgerConfigs);
        dLedgerProxy.startup();
        logger.info("DLedgers start ok with config {}", JSON.toJSONString(dLedgerConfigs));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;

            @Override
            public void run() {
                synchronized (this) {
                    logger.info("Shutdown hook was invoked");
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        dLedgerProxy.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        logger.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        }, "ShutdownHook"));
    }

    public static void bootstrapDLedger(DLedgerConfig dLedgerConfig) {
        bootstrapDLedger(Collections.singletonList(dLedgerConfig));
    }
}
