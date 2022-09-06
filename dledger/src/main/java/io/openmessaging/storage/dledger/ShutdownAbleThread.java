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

package io.openmessaging.storage.dledger;

import io.openmessaging.storage.dledger.utils.ResettableCountDownLatch;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

public abstract class ShutdownAbleThread extends Thread {
    protected final ResettableCountDownLatch waitPoint = new ResettableCountDownLatch(1);
    protected Logger logger;
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    private AtomicBoolean running = new AtomicBoolean(true);
    private CountDownLatch latch = new CountDownLatch(1);

    public ShutdownAbleThread(String name, Logger logger) {
        super(name);
        this.logger = logger;

    }

    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            try {
                wakeup();
                latch.await(10, TimeUnit.SECONDS);
            } catch (Throwable t) {
                if (logger != null) {
                    logger.error("Unexpected Error in shutting down {} ", getName(), t);
                }
            }
            if (latch.getCount() != 0) {
                if (logger != null) {
                    logger.error("The {} failed to shutdown in {} seconds", getName(), 10);
                }

            }
        }
    }

    public abstract void doWork();

    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    public void waitForRunning(long interval) throws InterruptedException {
        if (hasNotified.compareAndSet(true, false)) {
            return;
        }

        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("The {} is interrupted", getName(), e);
            throw e;
        } finally {
            hasNotified.set(false);
        }
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                doWork();
            } catch (Throwable t) {
                if (logger != null) {
                    logger.error("Unexpected Error in running {} ", getName(), t);
                }
            }
        }
        latch.countDown();
    }

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

}
