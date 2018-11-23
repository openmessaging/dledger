package io.openmessaging.storage.dleger.protocol;

import java.util.ArrayList;
import java.util.List;
import io.openmessaging.storage.dleger.entry.DLegerEntry;

public class GetEntriesResponse extends RequestOrResponse {
    private List<DLegerEntry> entries = new ArrayList<>();

    public List<DLegerEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<DLegerEntry> entries) {
        this.entries = entries;
    }
}
