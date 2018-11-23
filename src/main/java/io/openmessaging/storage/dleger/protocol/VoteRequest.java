package io.openmessaging.storage.dleger.protocol;

public class VoteRequest extends RequestOrResponse {

    private long legerEndIndex = -1;

    private long legerEndTerm = -1;

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
