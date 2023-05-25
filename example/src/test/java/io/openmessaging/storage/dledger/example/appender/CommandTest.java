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

package io.openmessaging.storage.dledger.example.appender;

import com.beust.jcommander.JCommander;
import io.openmessaging.storage.dledger.example.appender.command.ConfigCommand;
import io.openmessaging.storage.dledger.proxy.DLedgerProxyConfig;
import io.openmessaging.storage.dledger.proxy.util.ConfigUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class CommandTest {

    @Test
    public void TestParseArguments() {
        ConfigCommand configCommand = new ConfigCommand();
        JCommander.Builder builder = JCommander.newBuilder().addObject(configCommand);
        JCommander jc = builder.build();
        jc.parse("-c", "./src/test/resources/config.example.yaml");
        Assertions.assertEquals("./src/test/resources/config.example.yaml", configCommand.getConfigPath());
    }

    @Test
    public void TestGenerateProxyConfig() {
        ConfigCommand configCommand = new ConfigCommand();
        JCommander.Builder builder = JCommander.newBuilder().addObject(configCommand);
        JCommander jc = builder.build();
        jc.parse("-c", "./src/test/resources/config.example.yaml");
        Assertions.assertEquals("./src/test/resources/config.example.yaml", configCommand.getConfigPath());
        try {
            DLedgerProxyConfig config = ConfigUtils.parseDLedgerProxyConfig(configCommand.getConfigPath());
            Assertions.assertNotNull(config);
            Assertions.assertEquals(3, config.getConfigs().size());
            String s = "";
            for (int i = 0; i < config.getConfigs().size(); i++) {
                Assertions.assertEquals("127.0.0.1:10000", config.getConfigs().get(i).getSelfAddress());
                Assertions.assertEquals("g" + i, config.getConfigs().get(i).getGroup());
                s += config.getConfigs().get(i).getSelfId();
            }
            Assertions.assertEquals("n0a0b0", s);
        } catch (Exception e) {
            Assertions.assertNotNull(e);
        }
    }

}
