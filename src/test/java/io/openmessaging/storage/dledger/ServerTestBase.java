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

import io.openmessaging.storage.dledger.util.FileTestUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;

public class ServerTestBase {

    private static final AtomicInteger PORT_COUNTER = new AtomicInteger(30000);
    private static Random random = new Random();
    protected List<String> bases = new ArrayList<>();

    public static int nextPort() {
        return PORT_COUNTER.addAndGet(10 + random.nextInt(10));
    }

    @After
    public synchronized void shutdown() {
        for (String base : bases) {
            try {
                FileTestUtil.deleteFile(base);
            } catch (Throwable ignored) {

            }
        }
    }
}
