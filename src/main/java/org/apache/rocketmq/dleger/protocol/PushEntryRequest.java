package org.apache.rocketmq.dleger.protocol;

import org.apache.rocketmq.dleger.entry.DLegerEntry;

public class PushEntryRequest extends RequestOrResponse {
    public enum Type {
        WRITE,
        COMPARE,
        TRUNCATE;
    }
    private Type type = Type.WRITE;
    private Long term;

    private DLegerEntry entry;

    public Long getTerm() {
        return term;
    }

    public void setTerm(Long term) {
        this.term = term;
    }

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
}
