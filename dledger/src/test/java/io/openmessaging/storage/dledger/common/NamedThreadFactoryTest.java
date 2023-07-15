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

package io.openmessaging.storage.dledger.common;

import io.openmessaging.storage.dledger.common.NamedThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NamedThreadFactoryTest {

    @Test
    public void testNamedThreadFactory() {

        NamedThreadFactory threadFactory = new NamedThreadFactory("DledgerThread");
        Runnable runnable = () -> {
        };
        Thread thread = threadFactory.newThread(runnable);
        Assertions.assertEquals("DledgerThread-1", thread.getName());
        Assertions.assertFalse(thread.isDaemon());

        threadFactory = new NamedThreadFactory("DledgerThread", true);
        Thread thread1 = threadFactory.newThread(runnable);
        Assertions.assertEquals("DledgerThread-1", thread1.getName());
        Assertions.assertTrue(thread1.isDaemon());

        threadFactory = new NamedThreadFactory(null, "DledgerThread", true);
        Thread thread2 = threadFactory.newThread(runnable);
        Assertions.assertEquals("DledgerThread", thread2.getName());
        Assertions.assertTrue(thread2.isDaemon());

        threadFactory = new NamedThreadFactory(new AtomicInteger(0), "DledgerThread", true);
        Thread thread3 = threadFactory.newThread(runnable);
        Assertions.assertEquals("DledgerThread-1", thread3.getName());
        Assertions.assertTrue(thread2.isDaemon());

    }

}