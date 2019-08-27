/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.LeadershipTransferResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeadershipTransferCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(LeadershipTransferCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--leader", "-l"}, description = "set the current leader manually")
    private String leaderId;

    @Parameter(names = {"--transfereeId", "-t"}, description = "Node try to be the new leader")
    private String transfereeId = "n0";

    @Parameter(names = {"--term"}, description = "current term")
    private long term;

    @Override
    public void doCommand() {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        LeadershipTransferResponse response = dLedgerClient.leadershipTransfer(leaderId, transfereeId, term);
        logger.info("LeadershipTransfer code={}, Result:{}", DLedgerResponseCode.valueOf(response.getCode()),
            JSON.toJSONString(response));
        dLedgerClient.shutdown();
    }
}
