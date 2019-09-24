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
package io.openmessaging.storage.dledger;
import io.openmessaging.storage.dledger.protocol.AppendEntryRequest;
import io.openmessaging.storage.dledger.protocol.RequestOrResponse;
import java.util.concurrent.CompletableFuture;

public class ApplyTask<T extends RequestOrResponse> extends AppendEntryRequest {
    private CompletableFuture<T> responseFuture;

    private long expectTerm = -1;

    private long index;

    public ApplyTask(CompletableFuture<T> responseFuture, long index, long expectTerm) {
        this.responseFuture = responseFuture;
        this.index = index;
        this.expectTerm = expectTerm;
    }

    public CompletableFuture<T> getResponseFuture() {
        return responseFuture;
    }

    public void setResponseFuture(CompletableFuture<T> responseFuture) {
        this.responseFuture = responseFuture;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getExpectTerm() {
        return expectTerm;
    }

    public void setExpectTerm(long expectTerm) {
        this.expectTerm = expectTerm;
    }
}
