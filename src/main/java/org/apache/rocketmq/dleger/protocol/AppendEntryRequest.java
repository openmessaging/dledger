package org.apache.rocketmq.dleger.protocol;

public class AppendEntryRequest extends RequestOrResponse {

    private byte[] body;

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }
}
