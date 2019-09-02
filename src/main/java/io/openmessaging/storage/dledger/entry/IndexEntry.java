package io.openmessaging.storage.dledger.entry;

public class IndexEntry {

    private long prePos;
    private int size;
    private long nextIndex;

    public long getPrePos() {
        return prePos;
    }

    public void setPrePos(long prePos) {
        this.prePos = prePos;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }
}
