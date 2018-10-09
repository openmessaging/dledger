package org.apache.rocketmq.dleger.protocol;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.dleger.entry.DLegerEntry;

public class PullEntriesResponse extends RequestOrResponse {
    private List<DLegerEntry> entries = new ArrayList<>();

    public List<DLegerEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<DLegerEntry> entries) {
        this.entries = entries;
    }
}
