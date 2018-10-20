package org.apache.rocketmq.dleger.protocol;

public abstract class RequestOrResponse {

    protected String group;
    protected String remoteId;
    protected String localId;
    protected int code = DLegerResponseCode.SUCCESS.getCode();

    protected String leaderId = null;
    protected long term = -1;

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
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


    public String getRemoteId() {
        return remoteId;
    }

    public void setRemoteId(String remoteId) {
        this.remoteId = remoteId;
    }

    public String getLocalId() {
        return localId;
    }

    public void setLocalId(String localId) {
        this.localId = localId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }




    public String baseInfo() {
        return  String.format("rpc[group=%s,local=%s,leader=%s,term=%d,code=%d]", group, localId, leaderId, term, code);
    }

}
