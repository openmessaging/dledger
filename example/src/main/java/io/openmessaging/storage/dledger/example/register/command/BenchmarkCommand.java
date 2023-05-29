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

package io.openmessaging.storage.dledger.example.register.command;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.openmessaging.storage.dledger.example.common.command.BaseCommand;
import io.openmessaging.storage.dledger.example.register.RegisterBenchmark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "Benchmark the RegisterDLedger")
public class BenchmarkCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(BenchmarkCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--clientNum", "-c"}, description = "Client number")
    private int clientNum = 10;

    @Parameter(names = {"--ops", "-o"}, description = "Operation num per client")
    private long opsPerClient = 10000;

    @Parameter(names = {"--benchmarkType", "-t"}, description = "Benchmark type]")
    private RegisterBenchmark.BenchmarkType benchmarkType = RegisterBenchmark.BenchmarkType.Write;

    @Override
    public void doCommand() {
        RegisterBenchmark benchmark = new RegisterBenchmark(group, peers, clientNum, opsPerClient, benchmarkType);
        try {
            benchmark.start();
        } catch (Exception e) {
            logger.error("benchmark error", e);
        }
    }
}
