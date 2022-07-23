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

package io.openmessaging.storage.dledger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandDescription = "append data to DLedger server")
public class AppendCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(AppendCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--data", "-d"}, description = "the data to append")
    private List<String> data = new ArrayList<>();

    @Parameter(names = {"--count", "-c"}, description = "append several times")
    private int count = 1;

    @Override
    public void doCommand() {

        if (null == data || data.isEmpty()) {
            logger.warn("Not data append to  dledger server");
            return;
        }
        DLedgerClient dLedgerClient = new DLedgerClient(group, peers);
        dLedgerClient.startup();
        if (data.size() == 1) {
            byte[] dataBytes = data.get(0).getBytes();
            for (int i = 0; i < count; i++) {
                AppendEntryResponse response = dLedgerClient.append(dataBytes);
                logger.info("Append Result:{}", JSON.toJSONString(response));
            }
        } else {
            List<byte[]> dataList = data.stream().map(String::getBytes).collect(Collectors.toList());
            for (int i = 0; i < count; i++) {
                AppendEntryResponse response = dLedgerClient.batchAppend(dataList);
                logger.info("Append Result:{}", JSON.toJSONString(response));
            }
        }
        dLedgerClient.shutdown();
    }
}
