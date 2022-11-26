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

package io.openmessaging.storage.dledger.command;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.common.BaseCommand;
import io.openmessaging.storage.dledger.common.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.common.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.common.protocol.ReadFileResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "Read data from DLedger server data file")
public class ReadFileCommand implements BaseCommand {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--dir", "-d"}, description = "the data dir")
    private String dataDir = null;

    @Parameter(names = {"--pos", "-o"}, description = "the start position")
    private long pos = 0;

    @Parameter(names = {"--size", "-s"}, description = "the file size")
    private int size = -1;

    @Parameter(names = {"--index", "-i"}, description = "the index")
    private long index = -1L;

    @Parameter(names = {"--body", "-b"}, description = "if read the body")
    private boolean readBody = false;

    @Override
    public void doCommand() {
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        ReadFileResponse response = dLedgerClient.readFile(dataDir, pos, size, index, readBody);
        if (null == response || response.getCode() != DLedgerResponseCode.SUCCESS.getCode()) {
            LOGGER.warn(JSON.toJSONString(response));
            return;
        }
        DLedgerEntry entry = response.getDLedgerEntry();
        LOGGER.info(JSON.toJSONString(entry));

        dLedgerClient.shutdown();
    }
}
