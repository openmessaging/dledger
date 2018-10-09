package org.apache.rocketmq.dleger.protocol;

public class PushEntryResponse extends RequestOrResponse {
    private Long term;
    private Long index;

    public Long getTerm() {
        return term;
    }

    public void setTerm(Long term) {
        this.term = term;
    }

    public Long getIndex() {
        return index;
    }

    public void setIndex(Long index) {
        this.index = index;
    }
}
