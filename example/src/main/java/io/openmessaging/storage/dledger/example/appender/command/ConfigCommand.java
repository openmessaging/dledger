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

package io.openmessaging.storage.dledger.example.appender.command;

import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dledger.example.common.command.BaseCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(ConfigCommand.class);

    @Parameter(names = {"--config", "-c"}, description = "Config path of DLedger")
    private String config = "config.yaml";

    @Override
    public void doCommand() {
        //doCommand
    }

    public String getConfigPath() {
        return config;
    }
}
