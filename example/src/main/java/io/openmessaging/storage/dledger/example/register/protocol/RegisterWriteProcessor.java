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

package io.openmessaging.storage.dledger.example.register.protocol;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.protocol.DLedgerResponseCode;
import io.openmessaging.storage.dledger.protocol.userdefine.UserDefineProcessor;
import io.openmessaging.storage.dledger.utils.BytesUtil;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterWriteProcessor extends UserDefineProcessor<RegisterWriteRequest, RegisterWriteResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserDefineProcessor.class);

    public RegisterWriteProcessor(DLedgerServer dLedgerServer) {
        super(dLedgerServer);
    }

    @Override
    public CompletableFuture<RegisterWriteResponse> handleRequest(RegisterWriteRequest request) {
        Integer key = request.getKey();
        Integer value = request.getValue();
        byte[] body = encodeWriteEvent(key, value);
        return dLedgerServer.appendAsLeader(body).thenApply(appendResp -> {
            RegisterWriteResponse response = new RegisterWriteResponse();
            response.setTerm(appendResp.getTerm());
            response.setCode(appendResp.getCode());
            response.setLeaderId(appendResp.getLeaderId());
            return response;
        }).exceptionally(e -> {
            LOGGER.error("Failed to append bytes to dLedger", e);
            RegisterWriteResponse response = new RegisterWriteResponse();
            response.setCode(DLedgerResponseCode.UNKNOWN.getCode());
            return response;
        });
    }

    @Override
    public Integer getRequestTypeCode() {
        return RegisterRequestTypeCode.WRITE.ordinal();
    }

    private byte[] encodeWriteEvent(Integer key, Integer value) {
        byte[] keyBytes = BytesUtil.intToBytes(key);
        byte[] valueBytes = BytesUtil.intToBytes(value);
        byte[] afterCodingBytes = new byte[8];
        System.arraycopy(keyBytes, 0, afterCodingBytes, 0, 4);
        System.arraycopy(valueBytes, 0, afterCodingBytes, 4, 4);
        return afterCodingBytes;
    }
}
