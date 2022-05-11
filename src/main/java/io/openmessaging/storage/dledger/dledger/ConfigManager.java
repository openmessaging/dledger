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
package io.openmessaging.storage.dledger.dledger;

import io.openmessaging.storage.dledger.DLedgerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class ConfigManager {

    private static Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    private DLedgerProxyConfig dLedgerProxyConfig;

    //selfId -> DLedgerConfig
    private ConcurrentHashMap<String, DLedgerConfig> configMap;

    //selfId -> address
    private ConcurrentHashMap<String, String> addressMap;


    public ConfigManager(final DLedgerProxyConfig dLedgerProxyConfig) {
        this.dLedgerProxyConfig = dLedgerProxyConfig;
        initConfig();
    }

    public synchronized void addDLedgerConfig(DLedgerConfig dLedgerConfig) {
        this.configMap.put(dLedgerConfig.getSelfId(), dLedgerConfig);
        this.addressMap.putAll(dLedgerConfig.getPeerAddressMap());
    }

    public synchronized void removeDLedgerConfig(String selfId) {
        this.configMap.remove(selfId);
    }

    public DLedgerProxyConfig getdLedgerProxyConfig() {
        return dLedgerProxyConfig;
    }

    public void setdLedgerProxyConfig(DLedgerProxyConfig dLedgerProxyConfig) {
        this.dLedgerProxyConfig = dLedgerProxyConfig;
    }

    private void initConfig() {
        this.configMap = new ConcurrentHashMap<>();
        this.addressMap = new ConcurrentHashMap<>();
        for (DLedgerConfig config : this.dLedgerProxyConfig.getConfigs()) {
            this.configMap.put(config.getSelfId(), config);
            this.addressMap.putAll(config.getPeerAddressMap());
        }
    }

    public ConcurrentHashMap<String, DLedgerConfig> getConfigMap() {
        return configMap;
    }

    public String getAddress(String selfId) {
        return this.addressMap.get(selfId);
    }
}
