package org.apache.rocketmq.dleger.exception;

public class DLegerException extends RuntimeException {

    public enum Code {
        UNKNOWN,
        NOT_LEADER,
        NOT_FOLLOWER,
        UNCONSISTENCT_STATE,
        UNCONSISTENCT_TERM,
        UNCONSISTENCT_INDEX,
        UNCONSISTENCT_LEADER,
        INDEX_OUT_OF_RANGE,
        DISK_ERROR;
    }

    private String leaderId;
    private Code code;
    public DLegerException(Code code, String message, String leaderId) {
        super(message);
        this.leaderId = leaderId;
        this.code = code;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public Code getCode() {
        return code;
    }

    public void setCode(Code code) {
        this.code = code;
    }
}
