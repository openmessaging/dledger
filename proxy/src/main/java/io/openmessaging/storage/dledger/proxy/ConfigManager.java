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
package io.openmessaging.storage.dledger.proxy;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.concurrent.ConcurrentHashMap;

public class ConfigManager {

    //groupId#selfId -> DLedgerConfig
    private final ConcurrentHashMap<String, DLedgerConfig> configMap;

    //groupId#selfId -> address
    private final ConcurrentHashMap<String, String> addressMap;

    // ip:port
    private String listenAddress;

    private final ExecutorService executors = Executors.newFixedThreadPool(2, runnable -> {
        Thread t = new Thread(runnable);
        t.setDaemon(true);
        t.setName("ConfigManager-ListenerNotifyExecutor");
        return t;
    });

    // listen on the change of configs
    private final List<DLedgerProxyConfigListener> configChangeListeners = new LinkedList<>();

    public ConfigManager(List<DLedgerConfig> dLedgerConfigs) {
        preCheckDLedgerConfig(dLedgerConfigs);
        this.configMap = new ConcurrentHashMap<>();
        this.addressMap = new ConcurrentHashMap<>();
        for (DLedgerConfig config : dLedgerConfigs) {
            this.configMap.put(DLedgerUtils.generateDLedgerId(config.getGroup(), config.getSelfId()), config);
            this.addressMap.putAll(config.getPeerAddressMap());
        }
    }

    private void preCheckDLedgerConfig(List<DLedgerConfig> configs) {
        for (DLedgerConfig config : configs) {
            config.init();
            if (listenAddress != null && !listenAddress.equals(config.getSelfAddress())) {
                throw new IllegalArgumentException("[DLedgerConfigManager]: listen port error");
            }
            listenAddress = config.getSelfAddress();
        }
    }

    public void addDLedgerConfig(DLedgerConfig dLedgerConfig) {
        dLedgerConfig.init();
        this.configMap.put(DLedgerUtils.generateDLedgerId(dLedgerConfig.getGroup(), dLedgerConfig.getSelfId()), dLedgerConfig);
        this.addressMap.putAll(dLedgerConfig.getPeerAddressMap());
        this.executors.submit(() -> notifyListeners(dLedgerConfig, DLedgerProxyConfigListener.ConfigChangeEvent.ADD));
    }

    public void removeDLedgerConfig(String groupId, String selfId) {
        DLedgerConfig remove = this.configMap.remove(DLedgerUtils.generateDLedgerId(groupId, selfId));
        this.executors.submit(() -> notifyListeners(remove, DLedgerProxyConfigListener.ConfigChangeEvent.REMOVED));
    }

    private void notifyListeners(DLedgerConfig config, DLedgerProxyConfigListener.ConfigChangeEvent event) {
        for (DLedgerProxyConfigListener listener : this.configChangeListeners) {
            listener.onDLedgerConfigChange(config, event);
        }
    }

    public void registerConfigChangeListener(DLedgerProxyConfigListener listener) {
        this.configChangeListeners.add(listener);
    }

    public Map<String, DLedgerConfig> getConfigMap() {
        return new HashMap<>(this.configMap);
    }

    public String getAddress(String groupId, String selfId) {
        return this.addressMap.get(DLedgerUtils.generateDLedgerId(groupId, selfId));
    }

    public String getAddress(String dledgerId) {
        return this.addressMap.get(dledgerId);
    }

    public String getListenAddress() {
        return listenAddress;
    }
}
