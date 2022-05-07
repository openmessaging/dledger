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

    private ConcurrentHashMap<String, Map<String, DLedgerServer>> servers;

    public DLedgerManager(final DLedgerProxyConfig dLedgerProxyConfig, DLedgerRpcService dLedgerRpcService) {
        this.servers = new ConcurrentHashMap<>();
        initDLedgerServer(dLedgerProxyConfig, dLedgerRpcService);
    }

    private void initDLedgerServer(final DLedgerProxyConfig dLedgerProxyConfig, DLedgerRpcService dLedgerRpcService) {
        for (DLedgerConfig config : dLedgerProxyConfig.getConfigs()) {
            DLedgerServer server = new DLedgerServer(config);
            server.registerDLedgerRpcService(dLedgerRpcService);
            if (!servers.containsKey(config.getGroup())) {
                servers.put(config.getGroup(), new ConcurrentHashMap<>());
            }
            servers.get(config.getGroup()).put(config.getSelfId(), server);
        }
    }

    public DLedgerServer getDLedgerServer(final String groupId, final String selfId) {
        return this.servers.containsKey(groupId) ? this.servers.get(groupId).get(selfId) : null;
    }

    public void startup() {
        Iterator<Map.Entry<String, Map<String, DLedgerServer>>> iteratorServerList = servers.entrySet().iterator();
        while (iteratorServerList.hasNext()) {
            Map.Entry<String, Map<String, DLedgerServer>> next = iteratorServerList.next();
            Iterator<Map.Entry<String, DLedgerServer>> iteratorServer = next.getValue().entrySet().iterator();
            while (iteratorServer.hasNext()) {
                DLedgerServer server = iteratorServer.next().getValue();
                server.startup();
            }
        }
    }

    public void shutdown() {
        Iterator<Map.Entry<String, Map<String, DLedgerServer>>> iteratorServerList = servers.entrySet().iterator();
        while (iteratorServerList.hasNext()) {
            Map.Entry<String, Map<String, DLedgerServer>> next = iteratorServerList.next();
            Iterator<Map.Entry<String, DLedgerServer>> iteratorServer = next.getValue().entrySet().iterator();
            while (iteratorServer.hasNext()) {
                DLedgerServer server = iteratorServer.next().getValue();
                server.shutdown();
            }
        }
    }

    public List<DLedgerServer> getDLedgerServers() {
        final List<DLedgerServer> serverList = new ArrayList<DLedgerServer>();
        final Iterator<Map.Entry<String, Map<String, DLedgerServer>>> iteratorServerList = servers.entrySet().iterator();
        while (iteratorServerList.hasNext()) {
            final Map.Entry<String, Map<String, DLedgerServer>> next = iteratorServerList.next();
            final Iterator<Map.Entry<String, DLedgerServer>> iteratorServer = next.getValue().entrySet().iterator();
            while (iteratorServer.hasNext()) {
                DLedgerServer server = iteratorServer.next().getValue();
                serverList.add(server);
            }
        }
        return serverList;
    }
}
