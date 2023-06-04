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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger threadIndex;

    private final String threadNamePrefix;

    private final boolean isDaemonThread;

    public NamedThreadFactory(final String threadNamePrefix, boolean isDaemonThread) {
        this(new AtomicInteger(0), threadNamePrefix, isDaemonThread);
    }

    public NamedThreadFactory(AtomicInteger threadIndex, final String threadNamePrefix, boolean isDaemonThread) {
        this.threadIndex = threadIndex;
        this.threadNamePrefix = threadNamePrefix;
        this.isDaemonThread = isDaemonThread;
    }

    public NamedThreadFactory(final String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    /**
     * Constructs a new {@code Thread}.  Implementations may also initialize priority, name, daemon status, {@code
     * ThreadGroup}, etc.
     *
     * @param r a runnable to be executed by new thread instance
     * @return constructed thread, or {@code null} if the request to create a thread is rejected
     */
    @Override
    public Thread newThread(Runnable r) {

        StringBuilder threadName = new StringBuilder(threadNamePrefix);
        if (null != threadIndex) {
            threadName.append("-").append(threadIndex.incrementAndGet());
        }
        Thread thread = new Thread(r, threadName.toString());
        if (isDaemonThread) {
            thread.setDaemon(true);
        }
        return thread;
    }
}
