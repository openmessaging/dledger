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

package io.openmessaging.storage.dledger.proxy.util;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.proxy.DLedgerProxyConfig;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;

public class ConfigUtils {

    public static DLedgerProxyConfig parseDLedgerProxyConfig(final String path) throws Exception {
        DLedgerProxyConfig config = null;
        final Yaml yaml = new Yaml(new Constructor(DLedgerProxyConfig.class));
        InputStream inputStream = Files.newInputStream(Paths.get(path));
        config = yaml.load(inputStream);
        inputStream.close();
        if (config == null) {
            throw new IllegalArgumentException("DLedger Config doesn't exist");
        }
        if (!checkProxyConfig(config)) {
            throw new IllegalArgumentException("DLedger servers in DLedger Config don't have the same port");
        }
        return config;
    }

    public static boolean checkProxyConfig(DLedgerProxyConfig dLedgerProxyConfig) {
        // check same port
        String bindPort = null;
        for (DLedgerConfig config : dLedgerProxyConfig.getConfigs()) {
            config.init();
            String[] split = config.getSelfAddress().split(":");
            if (bindPort == null) {
                bindPort = split[1];
            } else {
                if (!bindPort.equals(split[1])) {
                    return false;
                }
            }
        }
        return true;
    }

}
