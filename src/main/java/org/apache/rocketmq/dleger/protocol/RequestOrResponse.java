package org.apache.rocketmq.dleger.protocol;

public abstract class RequestOrResponse {

    private String group;
    private String leaderId;
    private String remoteId;
    private int code = DLegerResponseCode.SUCCESS.getCode();

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public RequestOrResponse code(int code) {
        this.code =  code;
        return this;
    }
    public RequestOrResponse leaderId(String leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public String getRemoteId() {
        return remoteId;
    }

    public void setRemoteId(String remoteId) {
        this.remoteId = remoteId;
    }
}
