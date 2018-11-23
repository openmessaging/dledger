package io.openmessaging.storage.dleger.protocol;

import java.util.List;

public class GetEntriesRequest extends RequestOrResponse {
    private Long beginIndex;

    private int maxSize;

    private List<Long> indexList;

    public Long getBeginIndex() {
        return beginIndex;
    }

    public void setBeginIndex(Long beginIndex) {
        this.beginIndex = beginIndex;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public List<Long> getIndexList() {
        return indexList;
    }

    public void setIndexList(List<Long> indexList) {
        this.indexList = indexList;
    }
}
