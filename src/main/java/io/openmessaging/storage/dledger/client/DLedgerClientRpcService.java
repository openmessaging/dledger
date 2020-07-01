/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.client;

import io.openmessaging.storage.dledger.protocol.DLedgerClientProtocol;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class DLedgerClientRpcService implements DLedgerClientProtocol {
    private Map<String, String> peerMap = new ConcurrentHashMap<>();

    public void updatePeers(String peers) {
        for (String peerInfo : peers.split(";")) {
            String nodeId = peerInfo.split("-")[0];
            peerMap.put(nodeId, peerInfo.substring(nodeId.length() + 1));
        }
    }

    public void updatePeers(Map<String, String> peers) {
        peerMap.putAll(peers);
    }

    public String getPeerAddr(String id) {
        return peerMap.get(id);
    }

    public abstract void startup();

    public abstract void shutdown();
}
