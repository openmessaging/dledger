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

package io.openmessaging.storage.dledger.example.register.client;

import io.openmessaging.storage.dledger.common.ReadMode;
import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadRequest;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadResponse;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteRequest;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterDLedgerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterDLedgerClient.class);

    private final DLedgerClient client;

    public RegisterDLedgerClient(String group, String peers) {
        this.client = new DLedgerClient(group, peers);
    }

    public void startup() {
        client.startup();
    }

    public void shutdown() {
        client.shutdown();
    }

    public RegisterWriteResponse write(int key, int value) {
        RegisterWriteRequest request = new RegisterWriteRequest(key, value);
        return client.invokeUserDefineRequest(request, RegisterWriteResponse.class, true);
    }

    public RegisterReadResponse read(int key) {
        return this.read(key, ReadMode.RAFT_LOG_READ);
    }

    public RegisterReadResponse read(int key, String readModeStr) {
        ReadMode readMode = ReadMode.RAFT_LOG_READ;
        try {
            readMode = ReadMode.valueOf(readModeStr);
        } catch (Exception ignore) {
            LOGGER.error("Error readMode string: {}, use default readMode: ", readModeStr, ReadMode.RAFT_LOG_READ);
        }
        return this.read(key, readMode);
    }

    public RegisterReadResponse read(int key, ReadMode readMode) {
        RegisterReadRequest request = new RegisterReadRequest(key, readMode);
        return client.invokeUserDefineRequest(request, RegisterReadResponse.class, true);
    }

}
