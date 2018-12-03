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

import java.util.concurrent.CompletableFuture;

public class TimeoutFuture<T> extends CompletableFuture<T> {

    protected long createTimeMs = System.currentTimeMillis();

    protected long timeOutMs = 1000;

    public TimeoutFuture() {

    }

    public TimeoutFuture(long timeOutMs) {
        this.timeOutMs = timeOutMs;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }

    public void setCreateTimeMs(long createTimeMs) {
        this.createTimeMs = createTimeMs;
    }

    public long getTimeOutMs() {
        return timeOutMs;
    }

    public void setTimeOutMs(long timeOutMs) {
        this.timeOutMs = timeOutMs;
    }

    public boolean isTimeOut() {
        return System.currentTimeMillis() - createTimeMs >= timeOutMs;
    }

}
