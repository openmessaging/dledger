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
import io.openmessaging.storage.dledger.example.register.client.RegisterDLedgerClient;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "Write a key-value pair to RegisterDLedger")
public class WriteCommand extends BaseCommand {
    private static Logger logger = LoggerFactory.getLogger(WriteCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--key", "-k"}, description = "the key to set")
    private int key = 13;

    @Parameter(names = {"--value", "-v"}, description = "the value to set")
    private int value = 31;


    @Override
    public void doCommand() {
        RegisterDLedgerClient client = new RegisterDLedgerClient(group, peers);
        client.startup();
        RegisterWriteResponse response = client.write(key, value);
        logger.info("Write Result Code:{}", response.getCode());
        client.shutdown();
    }
}
