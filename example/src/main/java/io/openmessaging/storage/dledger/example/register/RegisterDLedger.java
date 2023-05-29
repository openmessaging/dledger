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

package io.openmessaging.storage.dledger.example.register;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadProcessor;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteProcessor;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterDLedger {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterDLedger.class);

    public static void bootstrap(DLedgerConfig dLedgerConfig) {
        DLedgerServer dLedgerServer = new DLedgerServer(dLedgerConfig);
        dLedgerServer.registerStateMachine(new RegisterStateMachine());
        dLedgerServer.registerUserDefineProcessors(
            Arrays.asList(new RegisterWriteProcessor(dLedgerServer), new RegisterReadProcessor(dLedgerServer))
        );
        dLedgerServer.startup();
        LOGGER.info("RegisterDLedger started");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;

            @Override
            public void run() {
                synchronized (this) {
                    LOGGER.info("Shutdown hook was invoked");
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        dLedgerServer.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        LOGGER.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        }, "ShutdownHook"));
    }
}
