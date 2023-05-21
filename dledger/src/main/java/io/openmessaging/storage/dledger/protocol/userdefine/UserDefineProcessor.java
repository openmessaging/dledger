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

package io.openmessaging.storage.dledger.protocol.userdefine;

import io.openmessaging.storage.dledger.DLedgerServer;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.concurrent.CompletableFuture;

public abstract class UserDefineProcessor<T extends UserDefineRequest, V extends UserDefineResponse> {

    protected final DLedgerServer dLedgerServer;

    public UserDefineProcessor(DLedgerServer dLedgerServer) {
        this.dLedgerServer = dLedgerServer;
    }

    public abstract CompletableFuture<V> handleRequest(T t);

    public abstract Integer getRequestTypeCode();

    public Type getRequestType() {
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        return parameterizedType.getActualTypeArguments()[0];
    }


    public Type getResponseType() {
        ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();
        return parameterizedType.getActualTypeArguments()[1];
    }

}
