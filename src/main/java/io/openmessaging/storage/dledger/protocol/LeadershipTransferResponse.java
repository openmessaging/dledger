package io.openmessaging.storage.dledger.protocol;

public class LeadershipTransferResponse extends RequestOrResponse {

    public LeadershipTransferResponse term(long term) {
        this.term = term;
        return this;
    }

    public LeadershipTransferResponse code(int code) {
        this.code = code;
        return this;
    }
}
