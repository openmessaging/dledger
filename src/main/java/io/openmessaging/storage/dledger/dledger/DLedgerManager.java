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
import io.openmessaging.storage.dledger.DLedgerRpcService;
import io.openmessaging.storage.dledger.DLedgerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DLedgerManager {

    private static Logger logger = LoggerFactory.getLogger(DLedgerManager.class);

    // groupId#selfId -> DLedgerServer
    private ConcurrentHashMap<String, DLedgerServer> servers;

    public DLedgerManager(final DLedgerProxyConfig dLedgerProxyConfig, final DLedgerRpcService dLedgerRpcService) {
        this.servers = new ConcurrentHashMap<>();
        initDLedgerServer(dLedgerProxyConfig, dLedgerRpcService);
    }

    private void initDLedgerServer(final DLedgerProxyConfig dLedgerProxyConfig, final DLedgerRpcService dLedgerRpcService) {
        for (DLedgerConfig config : dLedgerProxyConfig.getConfigs()) {
            DLedgerServer server = new DLedgerServer(config);
            server.registerDLedgerRpcService(dLedgerRpcService);
            servers.put(generateDLedgerId(config.getGroup(), config.getSelfId()), server);
        }
    }

    public DLedgerServer getDLedgerServer(final String groupId, final String selfId) {
        String key = generateDLedgerId(groupId, selfId);
        return this.servers.get(key);
    }

    public void startup() {
        final Iterator<Map.Entry<String, DLedgerServer>> iterator = servers.entrySet().iterator();
        while (iterator.hasNext()) {
            iterator.next().getValue().startup();
        }
    }

    public void shutdown() {
        final Iterator<Map.Entry<String, DLedgerServer>> iterator = servers.entrySet().iterator();
        while (iterator.hasNext()) {
            iterator.next().getValue().shutdown();
        }
    }

    public List<DLedgerServer> getDLedgerServers() {
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>();
        servers.entrySet().stream().forEach(x -> serverList.add(x.getValue()));
        return serverList;
    }

    public synchronized DLedgerServer addDLedgerServer(DLedgerConfig dLedgerConfig, DLedgerRpcService dLedgerRpcService) {
        DLedgerServer server = new DLedgerServer(dLedgerConfig);
        server.registerDLedgerRpcService(dLedgerRpcService);
        this.servers.put(generateDLedgerId(dLedgerConfig.getGroup(), dLedgerConfig.getSelfId()), server);
        return server;
    }

    public synchronized DLedgerServer removeDLedgerServer(DLedgerServer dLedgerServer) {
        dLedgerServer.shutdown();
        return this.servers.remove(generateDLedgerId(dLedgerServer.getdLedgerConfig().getGroup(), dLedgerServer.getdLedgerConfig().getSelfId()));
    }

    private String generateDLedgerId(final String groupId, final String selfId) {
        return new StringBuilder(20).append(groupId).append("#").append(selfId).toString();
    }

}
