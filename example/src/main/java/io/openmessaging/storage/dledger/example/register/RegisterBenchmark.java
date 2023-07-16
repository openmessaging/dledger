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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import io.openmessaging.storage.dledger.example.register.client.RegisterDLedgerClient;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadResponse;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteResponse;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
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

    private Slf4jReporter reporter;
    private Histogram latency;

    private Meter tps;

    private Counter errorCounter;

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
        MetricRegistry registry = new MetricRegistry();
        reporter = Slf4jReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        latency = registry.histogram("request_latency_ms");
        tps = registry.meter("request_tps");
        errorCounter = registry.counter("error_request_num");
    }

    public void start() throws Exception {

        final long totalOperation = clientNum * operationNumPerClient;

        final CyclicBarrier barrier = new CyclicBarrier(clientNum + 1);

        final String operationType = benchmarkType.name();

        for (int i = 0; i < clientNum; i++) {
            new Thread() {
                @Override
                public void run() {
                    RegisterDLedgerClient client = new RegisterDLedgerClient(group, peers);
                    client.startup();
                    try {
                        barrier.await();
                        for (int i = 0; i < operationNumPerClient; i++) {
                            int code = 200;
                            long start = System.currentTimeMillis();
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
                                tps.mark();
                                latency.update(DLedgerUtils.elapsed(start));
                            } else {
                                errorCounter.inc();
                            }
                        }
                        barrier.await();
                    } catch (Exception e) {
                        logger.error("client {} error", operationType, e);
                    } finally {
                        client.shutdown();
                    }
                }
            }.start();
        }
        barrier.await();
        StopWatch stopWatch = StopWatch.createStarted();
        barrier.await();
        final long cost = stopWatch.getTime(TimeUnit.MILLISECONDS);
        logger.info("Test type: {}, client num : {}, operation num per client: {}, total operation num: {}, cost: {}}",
            operationType, clientNum, operationNumPerClient, totalOperation, cost);
        reporter.report();
    }

}
