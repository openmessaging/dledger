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

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.cmdline.ConfigCommand;
import io.openmessaging.storage.dledger.dledger.DLedgerProxy;
import io.openmessaging.storage.dledger.dledger.DLedgerProxyConfig;
import io.openmessaging.storage.dledger.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DLedger {

    private static Logger logger = LoggerFactory.getLogger(DLedger.class);

    public static void main(String args[]) {
        Arrays.stream(args).forEach(System.out::print);
        DLedgerProxyConfig dLedgerProxyConfig = null;
        if ("--config".equals(args[0]) || "-c".equals(args[0])){
            ConfigCommand configCommand = new ConfigCommand();
            JCommander.newBuilder().addObject(configCommand).build().parse(args);
            try{
                dLedgerProxyConfig = ConfigUtils.parseDLedgerProxyConfig(configCommand.getConfigPath());
            }catch (IOException e){
                logger.error("Open config file error",e);
                System.exit(-1);
            }
        }else{
            DLedgerConfig dLedgerConfig = new DLedgerConfig();
            JCommander.newBuilder().addObject(dLedgerConfig).build().parse(args);
            dLedgerProxyConfig = new DLedgerProxyConfig();
            List<DLedgerConfig> dLedgerConfigs = new LinkedList<>();
            dLedgerConfigs.add(dLedgerConfig);
            dLedgerProxyConfig.setConfigs(dLedgerConfigs);
        }
        DLedgerProxy dLedgerProxy = new DLedgerProxy(dLedgerProxyConfig);
        dLedgerProxy.startup();
        logger.info("DLedgers start ok with config {}", JSON.toJSONString(dLedgerProxyConfig));
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
}
