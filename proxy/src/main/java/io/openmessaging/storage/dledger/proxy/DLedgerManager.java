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
import io.openmessaging.storage.dledger.DLedgerRpcService;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DLedgerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DLedgerManager.class);

    // groupId#selfId -> DLedgerServer
    private final ConcurrentHashMap<String, DLedgerServer> servers;

    private final DLedgerRpcService dLedgerRpcService;

    private final ConfigManager configManager;

    public DLedgerManager(final ConfigManager configManager, final DLedgerRpcService dLedgerRpcService) {
        this.configManager = configManager;
        this.servers = new ConcurrentHashMap<>();
        this.dLedgerRpcService = dLedgerRpcService;
        initDLedgerServer();
        // register configChange listener
        this.configManager.registerConfigChangeListener(this::updateDLedgerServer);
    }

    private void initDLedgerServer() {
        this.configManager.getConfigMap().forEach((dLedgerId, dLedgerConfig) -> {
            addDLedgerServer(dLedgerConfig, false);
        });
    }

    private void addDLedgerServer(DLedgerConfig dLedgerConfig, boolean start) {
        DLedgerServer dLedgerServer = new DLedgerServer(dLedgerConfig, this.dLedgerRpcService);
        this.servers.put(generateDLedgerId(dLedgerConfig.getGroup(), dLedgerConfig.getSelfId()), dLedgerServer);
        if (start) {
            dLedgerServer.startup();
        }
    }

    private void removeDLedgerServer(DLedgerConfig dLedgerConfig) {
        DLedgerServer removedServer = this.servers.remove(generateDLedgerId(dLedgerConfig.getGroup(), dLedgerConfig.getSelfId()));
        removedServer.shutdown();
    }

    private synchronized void updateDLedgerServer(DLedgerConfig addOrRemovedConfig,
        DLedgerProxyConfigListener.ConfigChangeEvent event) {
        switch (event) {
            case ADD:
                addDLedgerServer(addOrRemovedConfig, true);
                break;
            case REMOVED:
                removeDLedgerServer(addOrRemovedConfig);
                break;
            default:
                LOGGER.warn("unknown config change event: {}, changedConfig: {}", event, addOrRemovedConfig);
        }
    }

    public DLedgerServer getDLedgerServer(final String groupId, final String selfId) {
        String key = generateDLedgerId(groupId, selfId);
        return this.servers.get(key);
    }

    public void startup() {
        this.servers.forEach((dLedgerId, server) -> server.startup());
    }

    public void shutdown() {
        this.servers.forEach((dLedgerId, server) -> server.shutdown());
    }

    public List<DLedgerServer> getDLedgerServers() {
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>();
        servers.entrySet().stream().forEach(x -> serverList.add(x.getValue()));
        return serverList;
    }

    private String generateDLedgerId(final String groupId, final String selfId) {
        return new StringBuilder(20).append(groupId).append("#").append(selfId).toString();
    }

    public void registerStateMachine(StateMachine machine) {
        this.servers.get(machine.getBindDLedgerId()).registerStateMachine(machine);
    }
}
