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

import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.DLedger;
import io.openmessaging.storage.dledger.DLedgerConfig;
import java.util.HashMap;
import java.util.Map;

public class BossCommand {

    public static void main(String args[]) {
        Map<String, BaseCommand> commands = new HashMap<>();
        commands.put("append", new AppendCommand());
        commands.put("get", new GetCommand());
        commands.put("readFile", new ReadFileCommand());
        commands.put("leadershipTransfer", new LeadershipTransferCommand());

        JCommander.Builder builder = JCommander.newBuilder();
        builder.addCommand("server", new DLedgerConfig());
        for (String cmd : commands.keySet()) {
            builder.addCommand(cmd, commands.get(cmd));
        }
        JCommander jc = builder.build();
        jc.parse(args);

        if (jc.getParsedCommand() == null) {
            jc.usage();
        } else if (jc.getParsedCommand().equals("server")) {
            String[] subArgs = new String[args.length - 1];
            System.arraycopy(args, 1, subArgs, 0, subArgs.length);
            DLedger.main(subArgs);
        } else {
            BaseCommand command = commands.get(jc.getParsedCommand());
            if (command != null) {
                command.doCommand();
            } else {
                jc.usage();
            }
        }
    }
}
