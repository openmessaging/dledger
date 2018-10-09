package org.apache.rocketmq.dleger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;

public abstract class ShutdownAbleThread extends Thread {
    private Logger logger;
    private AtomicBoolean running = new AtomicBoolean(true);
    private CountDownLatch latch = new CountDownLatch(1);

    public ShutdownAbleThread(String name, Logger logger) {
        super(name);
        this.logger = logger;

    }

    public void shutdown() {
        if(running.compareAndSet(true, false)) {
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (Throwable t) {
                if (logger != null) {
                    logger.error("Unexpected Error in shutting down {} ",getName(), t);
                }
            }
            if (latch.getCount() != 0) {
                if (logger != null) {
                    logger.error("The {} failed to shutdown in {} seconds",getName(), 10);
                }

            }
        }
    }


    public abstract void doWork();


    public void run() {
        while (running.get()) {
            try {
                doWork();
            } catch (Throwable t) {
                if (logger != null) {
                    logger.error("Unexpected Error in running {} ",getName(), t);
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
