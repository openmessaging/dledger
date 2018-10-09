package org.apache.rocketmq.dleger.protocol;

public class HeartBeatRequest extends RequestOrResponse {
    private long term;


    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }
}
