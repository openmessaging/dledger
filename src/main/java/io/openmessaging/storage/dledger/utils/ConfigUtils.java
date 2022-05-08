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

package io.openmessaging.storage.dledger.utils;

import io.openmessaging.storage.dledger.dledger.DLedgerProxyConfig;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ConfigUtils {

    public static DLedgerProxyConfig parseDLedgerProxyConfig(final String path) throws IOException {
        DLedgerProxyConfig config = null;
        final Yaml yaml = new Yaml(new Constructor(DLedgerProxyConfig.class));
        InputStream inputStream = new FileInputStream(path);
        config = yaml.load(inputStream);
        inputStream.close();
        return config;
    }

}
