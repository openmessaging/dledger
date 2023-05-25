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

import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadResponse;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteResponse;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterBenchmark {

    private static Logger logger = LoggerFactory.getLogger(RegisterBenchmark.class);

    private String group;

    private String peers;

    private int clientNum;

    private long operationNumPerClient;

    private BenchmarkType benchmarkType;

    public enum BenchmarkType {
        Write,
        Read,
    }

    public RegisterBenchmark(String group, String peers, int clientNum, long operationNumPerClient,
        BenchmarkType benchmarkType) {
        this.group = group;
        this.peers = peers;
        this.clientNum = clientNum;
        this.operationNumPerClient = operationNumPerClient;
        this.benchmarkType = benchmarkType;
    }

    public void start() throws Exception {

        final long totalOperation = clientNum * operationNumPerClient;

        final CyclicBarrier barrier = new CyclicBarrier(clientNum + 1);

        final String operationType = benchmarkType.name();

        for (int i = 0; i < clientNum; i++) {
            new Thread() {
                @Override
                public void run() {
                    long success = 0;
                    long failNum = 0;
                    RegisterDLedgerClient client = new RegisterDLedgerClient(group, peers);
                    client.startup();
                    try {
                        barrier.await();
                        while (success < operationNumPerClient) {
                            int code = 200;
                            if (benchmarkType == BenchmarkType.Read) {
                                // Read
                                RegisterReadResponse resp = client.read(13);
                                code = resp.getCode();
                            } else {
                                // Write
                                RegisterWriteResponse resp = client.write(13, 13);
                                code = resp.getCode();
                            }
                            if (code == 200) {
                                success++;
                            } else {
                                failNum++;
                            }
                        }
                        barrier.await();
                        client.shutdown();
                    } catch (Exception e) {
                        logger.error("client {} error", operationType, e);
                    } finally {
                        logger.info("client {} finished, need {} total: {}, success: {}, fail: {}",
                            operationType, operationType, operationNumPerClient, success, failNum);
                        client.shutdown();
                    }
                }
            }.start();
        }
        barrier.await();
        StopWatch stopWatch = StopWatch.createStarted();
        barrier.await();
        final long cost = stopWatch.getTime(TimeUnit.MILLISECONDS);
        final long tps = Math.round(totalOperation * 1000 / cost);
        logger.info("Test type: {}, client num : {}, operation num per client: {}, total operation num: {}, cost: {}, tps: {}",
            operationType, clientNum, operationNumPerClient, totalOperation, cost, tps);
    }

}
