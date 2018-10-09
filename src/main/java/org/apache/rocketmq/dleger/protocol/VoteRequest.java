package org.apache.rocketmq.dleger.protocol;

public class VoteRequest extends RequestOrResponse {

    private long currTerm;

    private long legerEndIndex;

    private long legerEndTerm;


    public long getCurrTerm() {
        return currTerm;
    }

    public void setCurrTerm(long currTerm) {
        this.currTerm = currTerm;
    }

    public long getLegerEndIndex() {
        return legerEndIndex;
    }

    public void setLegerEndIndex(long legerEndIndex) {
        this.legerEndIndex = legerEndIndex;
    }

    public long getLegerEndTerm() {
        return legerEndTerm;
    }

    public void setLegerEndTerm(long legerEndTerm) {
        this.legerEndTerm = legerEndTerm;
    }
}
