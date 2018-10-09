package org.apache.rocketmq.dleger.protocol;

public class HeartBeatResponse extends RequestOrResponse {

    public long currTerm = -1;

    public long getCurrTerm() {
        return currTerm;
    }

    public void setCurrTerm(long currTerm) {
        this.currTerm = currTerm;
    }
    public HeartBeatResponse currTerm(long currTerm) {
        this.currTerm = currTerm;
        return this;
    }
}
