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

package io.openmessaging.storage.dledger.example;

import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.example.appender.command.AppendCommand;
import io.openmessaging.storage.dledger.example.appender.command.AppenderCommand;
import io.openmessaging.storage.dledger.example.appender.command.GetCommand;
import io.openmessaging.storage.dledger.example.common.command.BaseCommand;
import io.openmessaging.storage.dledger.example.common.command.LeadershipTransferCommand;
import io.openmessaging.storage.dledger.example.common.command.ReadFileCommand;
import io.openmessaging.storage.dledger.example.register.command.BenchmarkCommand;
import io.openmessaging.storage.dledger.example.register.command.ReadCommand;
import io.openmessaging.storage.dledger.example.register.command.RegisterCommand;
import io.openmessaging.storage.dledger.example.register.command.WriteCommand;
import java.util.HashMap;
import java.util.Map;

public class CommandCli {

    private final static Map<String, BaseCommand> COMMANDS = new HashMap<>();

    static {
        // for common command
        COMMANDS.put("leadershipTransfer", new LeadershipTransferCommand());
        COMMANDS.put("readFile", new ReadFileCommand());

        // for Appender
        COMMANDS.put("appender", new AppenderCommand());
        COMMANDS.put("append", new AppendCommand());
        COMMANDS.put("get", new GetCommand());

        // for Register
        COMMANDS.put("register", new RegisterCommand());
        COMMANDS.put("write", new WriteCommand());
        COMMANDS.put("read", new ReadCommand());
        COMMANDS.put("benchmark", new BenchmarkCommand());
    }



    public static void main(String[] args) {
        JCommander.Builder builder = JCommander.newBuilder();
        COMMANDS.forEach(builder::addCommand);
        JCommander jc = builder.build();
        jc.parse(args);

        if (jc.getParsedCommand() == null) {
            jc.usage();
        } else {
            BaseCommand command = COMMANDS.get(jc.getParsedCommand());
            if (null != command) {
                command.doCommand();
            } else {
                jc.usage();
            }
        }
    }

}
