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

package io.openmessaging.storage.dledger.statemachine.register;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.ReadClosure;
import io.openmessaging.storage.dledger.ReadMode;
import io.openmessaging.storage.dledger.Status;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineProcessor;

import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;

public class RegisterReadProcessor extends UserDefineProcessor<RegisterReadRequest, RegisterReadResponse> {

    private Integer requestTypeCode;

    private final DLedgerServer server;

    public RegisterReadProcessor(DLedgerServer server) {
        this.server = server;
        RegisterReadRequest registerReadRequest = new RegisterReadRequest(0);
        this.requestTypeCode = registerReadRequest.getRequestTypeCode();
    }
    @Override
    public CompletableFuture<RegisterReadResponse> handleRequest(RegisterReadRequest registerReadRequest) {
        Integer key = registerReadRequest.getKey();
        RegisterReadResponse response = new RegisterReadResponse();
        response.setKey(key);
        CompletableFuture<RegisterReadResponse> future = new CompletableFuture<>();
        ReadClosure closure = new ReadClosure() {
            @Override
            public void done(Status status) {
                if (status.isOk()) {
                    RegisterStateMachine registerStateMachine = (RegisterStateMachine) RegisterReadProcessor.this.server.getStateMachine();
                    Integer value = registerStateMachine.getValue(key);
                    response.setValue(value);
                    future.complete(response);
                } else {
                    response.setCode(status.code.getCode());
                    future.complete(response);
                }
            }
        };
        RegisterReadProcessor.this.server.handleRead(ReadMode.RAFT_LOG_READ, closure);
        return future;
    }

    @Override
    public Integer getRequestTypeCode() {
        return this.requestTypeCode;
    }

}
