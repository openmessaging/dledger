package org.apache.rocketmq.dleger;

import java.util.concurrent.CompletableFuture;

public class TimeoutFuture<T> extends CompletableFuture<T> {


    private long createTimeMs = System.currentTimeMillis();

    private long timeOutMs = 1000;


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
