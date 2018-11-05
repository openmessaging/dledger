package org.apache.rocketmq.dleger.protocol;

import org.apache.rocketmq.dleger.entry.DLegerEntry;

public class PushEntryRequest extends RequestOrResponse {
    public enum Type {
        APPEND,
        COMMIT,
        COMPARE,
        TRUNCATE;
    }
    private long commitIndex = -1;
    private Type type = Type.APPEND;

    private DLegerEntry entry;

    public DLegerEntry getEntry() {
        return entry;
    }

    public void setEntry(DLegerEntry entry) {
        this.entry = entry;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }
}
