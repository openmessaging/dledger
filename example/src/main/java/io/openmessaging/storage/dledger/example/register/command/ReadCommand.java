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

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.openmessaging.storage.dledger.common.ReadMode;
import io.openmessaging.storage.dledger.example.common.command.BaseCommand;
import io.openmessaging.storage.dledger.example.register.client.RegisterDLedgerClient;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "Read a key from the RegisterDLedger")
public class ReadCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(ReadCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--key", "-k"}, description = "The key to read")
    private int key = 13;

    @Parameter(names = {"--read-mode", "-r"}, description = "Read mode")
    private ReadMode readMode = ReadMode.RAFT_LOG_READ;


    @Override
    public void doCommand() {
        RegisterDLedgerClient client = new RegisterDLedgerClient(group, peers);
        client.startup();
        RegisterReadResponse response = client.read(key, readMode);
        logger.info("Get Result:{}", JSON.toJSONString(response));
        client.shutdown();
    }
}
