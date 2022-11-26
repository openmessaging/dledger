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

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.core.DLedgerConfig;
import io.openmessaging.storage.dledger.core.DLedgerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedgerBootstrap {
    private static Logger logger = LoggerFactory.getLogger(DLedgerBootstrap.class);

    public static void main(String args[]) {
        DLedgerConfig dLedgerConfig = new DLedgerConfig();
        JCommander.newBuilder().addObject(dLedgerConfig).build().parse(args);
        bootstrapDLedger(dLedgerConfig);
    }

    public static void bootstrapDLedger(DLedgerConfig dLedgerConfig) {

        if (null == dLedgerConfig) {
            logger.error("Bootstrap DLedger server error", new IllegalArgumentException("DLedgerConfig is null"));
            System.exit(-1);
        }

        DLedgerServer dLedgerServer = new DLedgerServer(dLedgerConfig);
        dLedgerServer.startup();
        logger.info("[{}] group {} start ok with config {}", dLedgerConfig.getSelfId(), dLedgerConfig.getGroup(), JSON.toJSONString(dLedgerConfig));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;

            @Override
            public void run() {
                synchronized (this) {
                    logger.info("Shutdown hook was invoked");
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        dLedgerServer.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        logger.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        }, "ShutdownHook"));
    }
}
