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

package io.openmessaging.storage.dleger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dleger.client.DLegerClient;
import io.openmessaging.storage.dleger.protocol.AppendEntryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(AppendCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";

    @Parameter(names = {"--data", "-d"}, description = "the data to append")
    private String data = "Hello";

    @Parameter(names = {"--count", "-c"}, description = "append several times")
    private int count = 1;

    @Override
    public void doCommand() {
        DLegerClient dLegerClient = new DLegerClient(group, peers);
        dLegerClient.startup();
        for (int i = 0; i < count; i++) {
            AppendEntryResponse response = dLegerClient.append(data.getBytes());
            logger.info("Append Result:{}", JSON.toJSONString(response));
        }
        dLegerClient.shutdown();
    }
}
