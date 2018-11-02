package org.apache.rocketmq.dleger.protocol;

public class AppendEntryResponse extends RequestOrResponse {

    private long index = -1;
    private long pos = -1;

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }
}
