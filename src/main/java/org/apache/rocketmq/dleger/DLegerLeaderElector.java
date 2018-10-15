package org.apache.rocketmq.dleger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.dleger.exception.DLegerException;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.HeartBeatRequest;
import org.apache.rocketmq.dleger.protocol.HeartBeatResponse;
import org.apache.rocketmq.dleger.protocol.VoteRequest;
import org.apache.rocketmq.dleger.protocol.VoteResponse;
import org.apache.rocketmq.dleger.store.DLegerStore;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLegerLeaderElector.class);

    private Random random = new Random();
    private DLegerConfig dLegerConfig;
    private MemberState memberState;
    private DLegerStore dLegerStore;
    private DLegerRpcService dLegerRpcService;


    //as a server handler
    //record the last leader state
    private long lastLeaderHeartBeatTime = -1;
    private long lastSendHeartBeatTime = -1;
    private int heartBeatTimeIntervalMs = 500;
    //as a client
    private long nextTimeToRequestVote = -1;
    private boolean needIncreaseTermImmediately = false;
    private int minVoteIntervalMs = 500;
    private int maxVoteIntervalMs = 1000;



    private VoteResponse.PARSE_RESULT lastParseResult = VoteResponse.PARSE_RESULT.WAIT_TO_REVOTE;

    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);



    public void startup() {
        stateMaintainer.start();
    }

    public void shutdown() {
        stateMaintainer.shutdown();
    }



    public DLegerLeaderElector(DLegerConfig dLegerConfig, MemberState memberState, DLegerStore dLegerStore, DLegerRpcService dLegerRpcService) {
        this.dLegerConfig = dLegerConfig;
        this.memberState =  memberState;
        this.dLegerStore = dLegerStore;
        this.dLegerRpcService = dLegerRpcService;
    }





    public CompletableFuture<HeartBeatResponse> heartBeatAsync(HeartBeatRequest request) throws Exception {
        if (request.getTerm() < memberState.currTerm()) {
            return CompletableFuture.completedFuture((HeartBeatResponse) new HeartBeatResponse().currTerm(memberState.currTerm()).code(DLegerResponseCode.REJECT_EXPIRED_TERM));
        } else if (request.getTerm() == memberState.currTerm()) {
            if (request.getLeaderId().equals(memberState.getLeaderId())) {
                lastLeaderHeartBeatTime = System.currentTimeMillis();
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }

        //abnormal case
        //hold the lock to get the latest term and leaderId
        synchronized (memberState) {
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture((HeartBeatResponse) new HeartBeatResponse().currTerm(memberState.currTerm()).code(DLegerResponseCode.REJECT_EXPIRED_TERM));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.getLeaderId() == null) {
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currtem {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture((HeartBeatResponse) new HeartBeatResponse().code(DLegerResponseCode.INTERNAL_ERROR));
                }
            } else {
                //To make it simple, for larger term, do not change to follower immediately
                //first change to candidate, and notify the state-maintainer thread
                changeRoleToCandidate(request.getTerm());
                //TOOD notify
                return CompletableFuture.completedFuture(new HeartBeatResponse());
            }
        }
    }


    public void changeRoleToLeader(long term) {
        memberState.changeToLeader(term);
        logger.info("[{}][ChangeRoleToLeader] from term: {} and currterm: {}", memberState.getSelfId(), term, memberState.currTerm());
        lastSendHeartBeatTime = -1;
    }

    public void changeRoleToCandidate(long term) {
        logger.info("[{}][ChangeRoleToCandidate] from term: {} and currterm: {}", memberState.getSelfId(), term, memberState.currTerm());
        memberState.changeToCandidate(term);
    }

    //just for test
    public void revote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currterm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
    }

    public CompletableFuture<VoteResponse> voteAsync(VoteRequest request) {
        //TODO should not throw exception
        //hold the lock to get the latest term, leaderId, legerEndIndex
        synchronized (memberState) {
            if (request.getCurrTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse().currTerm(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            }else if (request.getCurrTerm() == memberState.currTerm()) {
                if (memberState.currVoteFor()  == null) {
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //repeat just let it go
                } else {
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse().currTerm(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY__HAS_LEADER));
                    } else {
                        return CompletableFuture.completedFuture(new VoteResponse().currTerm(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                //stepped down by larger term
                changeRoleToCandidate(request.getCurrTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse().currTerm(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }
            //assert acceptedTerm is true
            if (request.getLegerEndTerm() < dLegerStore.getLegerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse().currTerm(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEGER_TERM));
            } else if (request.getLegerEndTerm() == dLegerStore.getLegerEndTerm() && request.getLegerEndIndex() < dLegerStore.getLegerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse().currTerm(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEGER_END_INDEX));
            }
            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse().currTerm(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }



    private void sendHearbeats(long term, String leaderId) throws Exception {

        for (String id: memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            try {
                HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
                heartBeatRequest.setLeaderId(leaderId);
                heartBeatRequest.setRemoteId(id);
                heartBeatRequest.setTerm(term);
                //maybe oneway is ok
                dLegerRpcService.heartBeat(heartBeatRequest);
            } catch (Exception e) {
                logger.warn("{}_[SEND_HEAT_BEAT] failed to {}", memberState.getSelfId(), id);
            }
        }
    }

    private void maintainAsLeader() throws Exception {
        if ((System.currentTimeMillis() - lastSendHeartBeatTime) >  heartBeatTimeIntervalMs - 100) {
            long term;
            String leaderId;
            synchronized (memberState) {
                if (!memberState.isLeader()) {
                    //stop sending
                    return;
                }
                term =  memberState.currTerm();
                leaderId = memberState.getLeaderId();
                lastSendHeartBeatTime = System.currentTimeMillis();
            }
            sendHearbeats(term, leaderId);
        }
    }

    private void maintainAsFollower() {
        if ((System.currentTimeMillis() -  lastLeaderHeartBeatTime) > heartBeatTimeIntervalMs + 100) {
            synchronized (memberState) {
                if (memberState.isFollower() && ((System.currentTimeMillis() -  lastLeaderHeartBeatTime) > heartBeatTimeIntervalMs + 100)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {}", memberState.getSelfId(), lastLeaderHeartBeatTime, heartBeatTimeIntervalMs);
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }


    private List<VoteResponse> voteForQuorumResponses(long term, long legerEndTerm, long legerEndIndex) {
        List<VoteResponse> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setLegerEndIndex(legerEndIndex);
            voteRequest.setLegerEndTerm(legerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setCurrTerm(term);
            voteRequest.setRemoteId(id);
            VoteResponse voteResponse;
            try {
                //TODO async
                if (memberState.getSelfId().equals(id)) {
                    voteResponse = voteAsync(voteRequest).get();
                    logger.info("[{}][HandleVote_{}] {} handleVote for {} in term {}", memberState.getSelfId(), voteResponse.getVoteResult(), memberState.getSelfId(), voteRequest.getLeaderId(), voteRequest.getCurrTerm());
                } else {
                    //async
                    voteResponse = dLegerRpcService.vote(voteRequest).get();
                }
            } catch (Exception e) {
                voteResponse = new VoteResponse();
            }
            responses.add(voteResponse);


        }
        return responses;
    }


    private void maintainAsCandidate() throws Exception {
        //for candidate
        if (System.currentTimeMillis() < nextTimeToRequestVote && !needIncreaseTermImmediately) {
            return;
        }
        long term;
        long legerEndTerm;
        long legerEndIndex;
        synchronized (memberState) {
            if (!memberState.isCandidate()) {
                return;
            }
            if (lastParseResult == VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT || needIncreaseTermImmediately) {
                long prevTerm = memberState.currTerm();
                term = memberState.nextTerm();
                logger.info("{}_[INCREASE_TERM] from {} to {}", memberState.getSelfId(), prevTerm, term);
            } else {
                term = memberState.currTerm();
            }
            legerEndIndex = dLegerStore.getLegerEndIndex();
            legerEndTerm = dLegerStore.getLegerEndTerm();
        }
        if (needIncreaseTermImmediately) {
            nextTimeToRequestVote = System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
            needIncreaseTermImmediately = false;
            return;
        }

        List<VoteResponse> quorumVoteResponses = voteForQuorumResponses(term, legerEndTerm, legerEndIndex);
        long knownMaxTermInGroup = -1;
        int allNum = 0;
        int acceptedNum = 0;
        int notReadyTermNum = 0;
        int biggerLegerNum = 0;
        boolean alreadyHasLeader = false;
        VoteResponse.PARSE_RESULT parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_REVOTE;

        for (VoteResponse response : quorumVoteResponses) {
            allNum++;
            switch (response.getVoteResult()) {
                case ACCEPT:
                    acceptedNum++;
                    break;
                case REJECT_ALREADY_VOTED:
                    break;
                case REJECT_ALREADY__HAS_LEADER:
                    alreadyHasLeader = true;
                case REJECT_EXPIRED_VOTE_TERM:
                    if (response.getCurrTerm() > knownMaxTermInGroup) {
                        knownMaxTermInGroup = response.getCurrTerm();
                    }
                    break;
                case REJECT_EXPIRED_LEGER_TERM:
                case REJECT_SMALL_LEGER_END_INDEX:
                    biggerLegerNum++;
                case REJECT_TERM_NOT_READY:
                    notReadyTermNum++;
                default:
                        break;

            }
        }

        if (knownMaxTermInGroup > term) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
            synchronized (memberState) {
                if (memberState.currTerm() < knownMaxTermInGroup) {
                    changeRoleToCandidate(knownMaxTermInGroup);
                }
            }
        } else if (alreadyHasLeader) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = System.currentTimeMillis() + heartBeatTimeIntervalMs + minVoteIntervalMs;
        } else if (!memberState.isQuorum(allNum)) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_REVOTE;
            nextTimeToRequestVote = System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
        } else if (memberState.isQuorum(acceptedNum)) {
            parseResult = VoteResponse.PARSE_RESULT.PASSED;
        } else if (memberState.isQuorum(acceptedNum + notReadyTermNum)) {
            parseResult = VoteResponse.PARSE_RESULT.REVOTE_IMMEDIATELY;
        } else if (memberState.isQuorum(acceptedNum + biggerLegerNum)) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_REVOTE;
            nextTimeToRequestVote = System.currentTimeMillis() + heartBeatTimeIntervalMs + minVoteIntervalMs;
        } else {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
        }
        lastParseResult = parseResult;
        logger.info("{}_[PARSE_VOTE_RESULT] term: {} allNum: {} acceptedNum: {} notReadyTermNum: {} biggerLegerNum: {} alreadyHasLeader: {} result: {}",
            memberState.getSelfId(), term, allNum, acceptedNum, notReadyTermNum, biggerLegerNum, alreadyHasLeader, parseResult);


        if (parseResult == VoteResponse.PARSE_RESULT.PASSED) {
            //handle the handleVote
            synchronized (memberState) {
                if (memberState.currTerm() == term) {
                    logger.info("{}_[VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
                    changeRoleToLeader(term);
                } else {
                    logger.warn("{}_[VOTE_RESULT] has been elected to be the leader in term {}, but currTerm is {}", memberState.getSelfId(), term, memberState.currTerm());
                }

            }
        }


    }


    private void maintainState() throws Exception {
        if (memberState.isLeader()) {
            maintainAsLeader();
        } else if (memberState.isFollower()) {
            maintainAsFollower();
        } else {
            maintainAsCandidate();
        }
    }



    public class StateMaintainer extends ShutdownAbleThread {

        public StateMaintainer(String name, Logger logger) {
            super(name, logger);
        }

        @Override public void doWork() {
            try {
                if (DLegerLeaderElector.this.dLegerConfig.isEnableLeaderElector()) {
                    DLegerLeaderElector.this.maintainState();
                }
                Thread.sleep(1);
            } catch (Throwable t) {
                DLegerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }

}
