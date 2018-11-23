package io.openmessaging.storage.dleger.protocol;

public class HeartBeatResponse extends RequestOrResponse {





    public HeartBeatResponse term(long term) {
        this.term = term;
        return this;
    }

    public HeartBeatResponse code(int code) {
        this.code = code;
        return this;
    }
}
