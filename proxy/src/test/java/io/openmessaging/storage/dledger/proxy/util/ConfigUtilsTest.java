/*
 * Copyright 2017-2022 The DLedger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.proxy.util;

import io.openmessaging.storage.dledger.proxy.DLedgerProxyConfig;
import io.openmessaging.storage.dledger.proxy.util.ConfigUtils;
import java.nio.file.NoSuchFileException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigUtilsTest {

    @Test
    public void TestConfigUtilsInit() throws Exception {
        DLedgerProxyConfig config = ConfigUtils.parseDLedgerProxyConfig("./src/test/resources/config.example.yaml");
        Assertions.assertEquals(3, config.getConfigs().size());
        for (int i = 0; i < config.getConfigs().size(); i++) {
            Assertions.assertEquals("127.0.0.1:10000", config.getConfigs().get(i).getSelfAddress());
            Assertions.assertEquals("g" + i, config.getConfigs().get(i).getGroup());
        }
    }

    @Test
    public void TestConfigUtilsInitErrorConfig() {
        DLedgerProxyConfig config = null;
        try {
            config = ConfigUtils.parseDLedgerProxyConfig("./src/test/resources/config.example.error.yaml");
        } catch (Exception e) {
            Assertions.assertNotNull(e);
            Assertions.assertEquals("DLedger servers in DLedger Config don't have the same port", e.getMessage());
        }
        Assertions.assertNull(config);
    }

    @Test
    public void TestConfigFileNotExist() {
        DLedgerProxyConfig config = null;
        try {
            config = ConfigUtils.parseDLedgerProxyConfig("./lcy.txt");
        }catch (Exception e) {
            Assertions.assertNotNull(e);
            Assertions.assertEquals(NoSuchFileException.class, e.getClass());
        }
    }

}
