/*
 * Copyright 2017-2020 the original author or authors.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLedger {

    private static Logger logger = LoggerFactory.getLogger(DLedger.class);

    public static void main(String args[]) {
        DLedgerConfig dLedgerConfig = new DLedgerConfig();
        JCommander.newBuilder().addObject(dLedgerConfig).build().parse(args);
        DLedgerServer dLedgerServer = new DLedgerServer(dLedgerConfig);
        dLedgerServer.startup();
        logger.info("[{}] group {} start ok with config {}", dLedgerConfig.getSelfId(), dLedgerConfig.getGroup(), JSON.toJSONString(dLedgerConfig));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook was invoked");
            long beginTime = System.currentTimeMillis();
            dLedgerServer.shutdown();
            long consumingTimeTotal = System.currentTimeMillis() - beginTime;
            logger.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
        }, "ShutdownHook"));
    }
}
