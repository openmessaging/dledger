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

package io.openmessaging.storage.dledger.example.register;

import io.openmessaging.storage.dledger.client.DLedgerClient;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadRequest;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterReadResponse;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteRequest;
import io.openmessaging.storage.dledger.example.register.protocol.RegisterWriteResponse;

public class RegisterDLedgerClient {

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
        RegisterReadRequest request = new RegisterReadRequest(key);
        return client.invokeUserDefineRequest(request, RegisterReadResponse.class, true);
    }

}
