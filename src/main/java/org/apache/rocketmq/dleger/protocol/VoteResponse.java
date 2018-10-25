package org.apache.rocketmq.dleger.protocol;

import static org.apache.rocketmq.dleger.protocol.VoteResponse.RESULT.UNKNOWN;

public class VoteResponse extends RequestOrResponse {

    public enum RESULT {
        UNKNOWN,
        ACCEPT,
        REJECT_UNKNOWN_LEADER,
        REJECT_UNEXPECTED_LEADER,
        REJECT_EXPIRED_VOTE_TERM,
        REJECT_ALREADY_VOTED,
        REJECT_ALREADY__HAS_LEADER,
        REJECT_TERM_NOT_READY,
        REJECT_TERM_SMALL_THAN_LEGER,
        REJECT_EXPIRED_LEGER_TERM,
        REJECT_SMALL_LEGER_END_INDEX;
    }

    public enum PARSE_RESULT {
        WAIT_TO_REVOTE,
        REVOTE_IMMEDIATELY,
        PASSED,
        WAIT_TO_VOTE_NEXT;
    }

    public VoteResponse() {

    }

    public VoteResponse(VoteRequest request) {
        copyBaseInfo(request);
    }


    public RESULT voteResult = UNKNOWN;

    public RESULT getVoteResult() {
        return voteResult;
    }

    public void setVoteResult(RESULT voteResult) {
        this.voteResult = voteResult;
    }



    public VoteResponse voteResult(RESULT voteResult) {
        this.voteResult = voteResult;
        return this;
    }
    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }
}
