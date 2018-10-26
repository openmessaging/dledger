package org.apache.rocketmq.dleger;

import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.protocol.HeartBeatRequest;
import org.apache.rocketmq.dleger.protocol.HeartBeatResponse;
import org.apache.rocketmq.dleger.protocol.VoteRequest;
import org.apache.rocketmq.dleger.protocol.VoteResponse;
import org.apache.rocketmq.dleger.utils.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLegerLeaderElector {

    private static Logger logger = LoggerFactory.getLogger(DLegerLeaderElector.class);

    private Random random = new Random();
    private DLegerConfig dLegerConfig;
    private MemberState memberState;
    private DLegerRpcService dLegerRpcService;


    //as a server handler
    //record the last leader state
    private long lastLeaderHeartBeatTime = -1;
    private long lastSendHeartBeatTime = -1;
    private long lastSuccHeartBeatTime = -1;
    private int heartBeatTimeIntervalMs = 1000;
    //as a client
    private long nextTimeToRequestVote = -1;
    private boolean needIncreaseTermImmediately = false;
    private int minVoteIntervalMs = 300;
    private int maxVoteIntervalMs = 1000;



    private VoteResponse.PARSE_RESULT lastParseResult = VoteResponse.PARSE_RESULT.WAIT_TO_REVOTE;

    private StateMaintainer stateMaintainer = new StateMaintainer("StateMaintainer", logger);



    public void startup() {
        stateMaintainer.start();
    }

    public void shutdown() {
        stateMaintainer.shutdown();
    }



    public DLegerLeaderElector(DLegerConfig dLegerConfig, MemberState memberState, DLegerRpcService dLegerRpcService) {
        this.dLegerConfig = dLegerConfig;
        this.memberState =  memberState;
        this.dLegerRpcService = dLegerRpcService;
        refreshIntervals(dLegerConfig);
    }


    private void refreshIntervals(DLegerConfig dLegerConfig) {
        this.heartBeatTimeIntervalMs = dLegerConfig.getHeartBeatTimeIntervalMs();
        this.minVoteIntervalMs = dLegerConfig.getMinVoteIntervalMs();
        this.maxVoteIntervalMs = dLegerConfig.getMaxVoteIntervalMs();
    }





    public CompletableFuture<HeartBeatResponse> handleHeartBeat(HeartBeatRequest request) throws Exception {
        if (request.getTerm() < memberState.currTerm()) {
            return CompletableFuture.completedFuture((HeartBeatResponse) new HeartBeatResponse().term(memberState.currTerm()).code(DLegerResponseCode.EXPIRED_TERM.getCode()));
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
                return CompletableFuture.completedFuture((HeartBeatResponse) new HeartBeatResponse().term(memberState.currTerm()).code(DLegerResponseCode.EXPIRED_TERM.getCode()));
            } else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.getLeaderId() == null) {
                    changeRoleToFollower(request.getTerm(), request.getLeaderId());
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else if (request.getLeaderId().equals(memberState.getLeaderId())) {
                    lastLeaderHeartBeatTime = System.currentTimeMillis();
                    return CompletableFuture.completedFuture(new HeartBeatResponse());
                } else {
                    //this should not happen, but if happened
                    logger.error("[{}][BUG] currterm {} has leader {}, but received leader {}", memberState.getSelfId(), memberState.currTerm(), memberState.getLeaderId(), request.getLeaderId());
                    return CompletableFuture.completedFuture((HeartBeatResponse) new HeartBeatResponse().code(DLegerResponseCode.INCONSISTENT_LEADER.getCode()));
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
        synchronized (memberState) {
            if (memberState.currTerm() == term) {
                memberState.changeToLeader(term);
                lastSendHeartBeatTime = -1;
                logger.info("[{}] [ChangeRoleToLeader] from term: {} and currterm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.warn("[{}] skip to be the leader in term: {}, but currTerm is: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    public void changeRoleToCandidate(long term) {
        synchronized (memberState) {
            if (term >= memberState.currTerm()) {
                memberState.changeToCandidate(term);
                logger.info("[{}] [ChangeRoleToCandidate] from term: {} and currterm: {}", memberState.getSelfId(), term, memberState.currTerm());
            } else {
                logger.info("[{}] skip to be candidate in term: {}, but currterm: {}", memberState.getSelfId(), term, memberState.currTerm());
            }
        }
    }

    //just for test
    public void testRevote(long term) {
        changeRoleToCandidate(term);
        lastParseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
        nextTimeToRequestVote = -1;
    }

    public void changeRoleToFollower(long term, String leaderId) {
        logger.info("[{}][ChangeRoleToFollower] from term: {} leaderId: {} and currterm: {}", memberState.getSelfId(), term, leaderId, memberState.currTerm());
        memberState.changeToFollower(term, leaderId);
        lastLeaderHeartBeatTime = System.currentTimeMillis();
    }

    public CompletableFuture<VoteResponse> handleVote(VoteRequest request, boolean self) {
        //hold the lock to get the latest term, leaderId, legerEndIndex
        synchronized (memberState) {
            if (!memberState.isPeerMember(request.getLeaderId())) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNKNOWN_LEADER));
            }
            if (!self && memberState.getSelfId().equals(request.getLeaderId())) {
                logger.warn("[BUG]{} get vote from remote {}", request.getLeaderId(), request.getLeaderId());
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_UNEXPECTED_LEADER));
            }
            if (request.getTerm() < memberState.currTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_VOTE_TERM));
            }else if (request.getTerm() == memberState.currTerm()) {
                if (memberState.currVoteFor()  == null) {
                    //let it go
                } else if (memberState.currVoteFor().equals(request.getLeaderId())) {
                    //repeat just let it go
                } else {
                    if (memberState.getLeaderId() != null) {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY__HAS_LEADER));
                    } else {
                        return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_ALREADY_VOTED));
                    }
                }
            } else {
                //stepped down by larger term
                changeRoleToCandidate(request.getTerm());
                needIncreaseTermImmediately = true;
                //only can handleVote when the term is consistent
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_NOT_READY));
            }

            //assert acceptedTerm is true
            if (request.getLegerEndTerm() < memberState.getLegerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_EXPIRED_LEGER_TERM));
            } else if (request.getLegerEndTerm() == memberState.getLegerEndTerm() && request.getLegerEndIndex() < memberState.getLegerEndIndex()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.REJECT_SMALL_LEGER_END_INDEX));
            }

            if (request.getTerm() < memberState.getLegerEndTerm()) {
                return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.getLegerEndTerm()).voteResult(VoteResponse.RESULT.REJECT_TERM_SMALL_THAN_LEGER));
            }

            memberState.setCurrVoteFor(request.getLeaderId());
            return CompletableFuture.completedFuture(new VoteResponse(request).term(memberState.currTerm()).voteResult(VoteResponse.RESULT.ACCEPT));
        }
    }



    private void sendHeartbeats(long term, String leaderId) throws Exception {
        final AtomicInteger allNum = new AtomicInteger(1);
        final AtomicInteger succNum = new AtomicInteger(1);
        final AtomicLong maxTerm = new AtomicLong(-1);
        final AtomicBoolean inconsistLeader = new AtomicBoolean(false);
        final CountDownLatch beatLatch = new CountDownLatch(1);
        for (String id: memberState.getPeerMap().keySet()) {
            if (memberState.getSelfId().equals(id)) {
                continue;
            }
            HeartBeatRequest heartBeatRequest = new HeartBeatRequest();
            heartBeatRequest.setLocalId(memberState.getSelfId());
            heartBeatRequest.setRemoteId(id);
            heartBeatRequest.setLeaderId(leaderId);
            heartBeatRequest.setTerm(term);
            CompletableFuture<HeartBeatResponse> future = dLegerRpcService.heartBeat(heartBeatRequest);
            future.whenComplete((HeartBeatResponse x, Throwable ex) -> {
               try {

                   if (ex != null) {
                       throw ex;
                   }
                   switch (DLegerResponseCode.valueOf(x.getCode())) {
                       case SUCCESS:
                           succNum.incrementAndGet();
                           break;
                       case EXPIRED_TERM:
                           maxTerm.set(x.getTerm());
                           break;
                       case INCONSISTENT_LEADER:
                           inconsistLeader.compareAndSet(false, true);
                           break;
                       default:
                           break;
                   }
                   if (memberState.isQuorum(succNum.get())) {
                       beatLatch.countDown();
                   }
               } catch (Throwable t) {
                   logger.error("Parse heartbeat response failed", t);
               } finally {
                   allNum.incrementAndGet();
                   if (allNum.get() == memberState.peerSize()) {
                       beatLatch.countDown();
                   }
               }
            });
        }
        beatLatch.await(heartBeatTimeIntervalMs, TimeUnit.MILLISECONDS);
        if (memberState.isQuorum(succNum.get())) {
            lastSuccHeartBeatTime = System.currentTimeMillis();
        } else if (maxTerm.get() > term) {
            changeRoleToCandidate(maxTerm.get());
        } else if (inconsistLeader.get()) {
            changeRoleToCandidate(term);
        } else if (UtilAll.elapsed(lastSuccHeartBeatTime) > 3 * heartBeatTimeIntervalMs) {
            changeRoleToCandidate(term);
        }
    }

    private void maintainAsLeader() throws Exception {
        if (UtilAll.elapsed(lastSendHeartBeatTime) >  heartBeatTimeIntervalMs) {
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
            sendHeartbeats(term, leaderId);
        }
    }

    private void maintainAsFollower() {
        if (UtilAll.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs) {
            synchronized (memberState) {
                if (memberState.isFollower() && (UtilAll.elapsed(lastLeaderHeartBeatTime) > 2 * heartBeatTimeIntervalMs)) {
                    logger.info("[{}][HeartBeatTimeOut] lastLeaderHeartBeatTime: {} heartBeatTimeIntervalMs: {}", memberState.getSelfId(), lastLeaderHeartBeatTime, heartBeatTimeIntervalMs);
                    changeRoleToCandidate(memberState.currTerm());
                }
            }
        }
    }


    private List<CompletableFuture<VoteResponse>> voteForQuorumResponses(long term, long legerEndTerm, long legerEndIndex) throws Exception {
        List<CompletableFuture<VoteResponse>> responses = new ArrayList<>();
        for (String id : memberState.getPeerMap().keySet()) {
            VoteRequest voteRequest = new VoteRequest();
            voteRequest.setLegerEndIndex(legerEndIndex);
            voteRequest.setLegerEndTerm(legerEndTerm);
            voteRequest.setLeaderId(memberState.getSelfId());
            voteRequest.setTerm(term);
            voteRequest.setRemoteId(id);
            CompletableFuture<VoteResponse> voteResponse;
            if (memberState.getSelfId().equals(id)) {
                voteResponse = handleVote(voteRequest, true);
            } else {
                //async
                voteResponse = dLegerRpcService.vote(voteRequest);
            }
            responses.add(voteResponse);


        }
        return responses;
    }


    private long getNextTimeToRequestVote() {
        return System.currentTimeMillis() + minVoteIntervalMs + random.nextInt(maxVoteIntervalMs - minVoteIntervalMs);
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
            legerEndIndex = memberState.getLegerEndIndex();
            legerEndTerm = memberState.getLegerEndTerm();
        }
        if (needIncreaseTermImmediately) {
            nextTimeToRequestVote = getNextTimeToRequestVote();
            needIncreaseTermImmediately = false;
            return;
        }

        final List<CompletableFuture<VoteResponse>> quorumVoteResponses = voteForQuorumResponses(term, legerEndTerm, legerEndIndex);
        final AtomicLong knownMaxTermInGroup = new AtomicLong(-1);
        final AtomicInteger allNum = new AtomicInteger(0);
        final AtomicInteger validNum = new AtomicInteger(0);
        final AtomicInteger acceptedNum = new AtomicInteger(0);
        final AtomicInteger notReadyTermNum = new AtomicInteger(0);
        final AtomicInteger biggerLegerNum = new AtomicInteger(0);
        final AtomicBoolean alreadyHasLeader = new AtomicBoolean(false);

        CountDownLatch voteLatch = new CountDownLatch(1);
        for (CompletableFuture<VoteResponse> future: quorumVoteResponses) {
            future.whenComplete((VoteResponse x, Throwable ex) -> {
                try {
                    if (ex != null) {
                        throw ex;
                    }
                    logger.info("[{}][GetVoteResponse] {}", memberState.getSelfId(), JSON.toJSONString(x));
                    if (x.getVoteResult() != VoteResponse.RESULT.UNKNOWN) {
                        validNum.incrementAndGet();
                    }
                    synchronized (knownMaxTermInGroup) {
                        switch (x.getVoteResult()) {
                            case ACCEPT:
                                acceptedNum.incrementAndGet();
                                break;
                            case REJECT_ALREADY_VOTED:
                                break;
                            case REJECT_ALREADY__HAS_LEADER:
                                alreadyHasLeader.compareAndSet(false, true);
                                break;
                            case REJECT_TERM_SMALL_THAN_LEGER:
                            case REJECT_EXPIRED_VOTE_TERM:
                                if (x.getTerm() > knownMaxTermInGroup.get()) {
                                    knownMaxTermInGroup.set(x.getTerm());
                                }
                                break;
                            case REJECT_EXPIRED_LEGER_TERM:
                            case REJECT_SMALL_LEGER_END_INDEX:
                                biggerLegerNum.incrementAndGet();
                                break;
                            case REJECT_TERM_NOT_READY:
                                notReadyTermNum.incrementAndGet();
                                break;
                            default:
                                break;

                        }
                    }
                    if (alreadyHasLeader.get()
                        || memberState.isQuorum(acceptedNum.get())
                        || memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
                        voteLatch.countDown();
                    }
                } catch (Throwable t) {
                    logger.error("Get error when parsing vote response ", t);
                } finally {
                    allNum.incrementAndGet();
                    if (allNum.get() == memberState.peerSize()) {
                        voteLatch.countDown();
                    }
                }
            });

        }
        try {
            voteLatch.await(3, TimeUnit.SECONDS);
        } catch (Throwable ignore) {

        }
        VoteResponse.PARSE_RESULT parseResult;
        if (knownMaxTermInGroup.get() > term) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
            changeRoleToCandidate(knownMaxTermInGroup.get());
        } else if (alreadyHasLeader.get()) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (!memberState.isQuorum(validNum.get())) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else if (memberState.isQuorum(acceptedNum.get())) {
            parseResult = VoteResponse.PARSE_RESULT.PASSED;
        } else if (memberState.isQuorum(acceptedNum.get() + notReadyTermNum.get())) {
            parseResult = VoteResponse.PARSE_RESULT.REVOTE_IMMEDIATELY;
        } else if (memberState.isQuorum(acceptedNum.get() + biggerLegerNum.get())) {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_REVOTE;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        } else {
            parseResult = VoteResponse.PARSE_RESULT.WAIT_TO_VOTE_NEXT;
            nextTimeToRequestVote = getNextTimeToRequestVote();
        }
        lastParseResult = parseResult;
        logger.info("{}_[PARSE_VOTE_RESULT] term: {} memberNum:{} allNum: {} acceptedNum: {} notReadyTermNum: {} biggerLegerNum: {} alreadyHasLeader: {} result: {}",
            memberState.getSelfId(), term, memberState.getPeerMap().size(), allNum, acceptedNum, notReadyTermNum, biggerLegerNum, alreadyHasLeader, parseResult);

        if (parseResult == VoteResponse.PARSE_RESULT.PASSED) {
            logger.info("{}_[VOTE_RESULT] has been elected to be the leader in term {}", memberState.getSelfId(), term);
            changeRoleToLeader(term);
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
                    DLegerLeaderElector.this.refreshIntervals(dLegerConfig);
                    DLegerLeaderElector.this.maintainState();
                }
                Thread.sleep(1);
            } catch (Throwable t) {
                DLegerLeaderElector.logger.error("Error in heartbeat", t);
            }
        }

    }

}
