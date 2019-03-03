package io.openmessaging.storage.dledger.protocol;

public class LeadershipTransferRequest extends RequestOrResponse {

    private String transferId;
    private String transfereeId;
    private long takeLeadershipLedgerIndex;

    public String getTransfereeId() {
        return transfereeId;
    }

    public void setTransfereeId(String transfereeId) {
        this.transfereeId = transfereeId;
    }

    public String getTransferId() {
        return transferId;
    }

    public void setTransferId(String transferId) {
        this.transferId = transferId;
    }

    public long getTakeLeadershipLedgerIndex() {
        return takeLeadershipLedgerIndex;
    }

    public void setTakeLeadershipLedgerIndex(long takeLeadershipLedgerIndex) {
        this.takeLeadershipLedgerIndex = takeLeadershipLedgerIndex;
    }
}
