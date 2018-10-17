package org.apache.rocketmq.dleger.protocol;

public class HeartBeatRequest extends RequestOrResponse {
    public HeartBeatRequest term(long term) {
        this.term = term;
        return this;
    }
}
