package org.apache.rocketmq.dleger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.dleger.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemberState {

    public static Logger logger = LoggerFactory.getLogger(MemberState.class);



    public static final int LEADER = 1;
    public static final int CANDIDATE = 2;
    public static final int FOLLOWER = 3;

    public static final String TERM_PERSIST_FILE = "currterm";
    public static final String TERM_PERSIST_KEY_TERM = "currTerm";
    public static final String TERM_PERSIST_KEY_VOTE_FOR = "voteLeader";


    private ReentrantLock defaultLock = new ReentrantLock();

    public DLegerConfig dLegerConfig;

    private AtomicInteger role = new AtomicInteger(CANDIDATE);


    private String group;
    private String selfId;
    private String peers;

    private String leaderId;

    private long currTerm = -1;

    private String currVoteFor;

    private long knownMaxTermInGroup = -1;

    private Map<String, String> peerMap = new HashMap<>();
    private Map<String, Long> syncIndex = new ConcurrentHashMap<>();



    public MemberState(DLegerConfig config) {
        this.group = config.getGroup();
        this.selfId = config.getSelfId();
        this.peers = config.getPeers();
        for (String peerInfo: this.peers.split(";")) {
            peerMap.put(peerInfo.split("-")[0], peerInfo.split("-")[1]);
        }
        this.dLegerConfig = config;
        loadTerm();
    }

    private void loadTerm() {
        try {
            String data = IOUtils.file2String(dLegerConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
            Properties properties = IOUtils.string2Properties(data);
            if (properties == null) {
                return;
            }
            if (properties.containsKey(TERM_PERSIST_KEY_TERM)) {
                currTerm = Long.valueOf(String.valueOf(properties.get(TERM_PERSIST_KEY_TERM)));
            }
            if (properties.containsKey(TERM_PERSIST_KEY_VOTE_FOR)) {
                currVoteFor = String.valueOf(properties.get(TERM_PERSIST_KEY_VOTE_FOR));
                if (currVoteFor.length() == 0) {
                    currVoteFor = null;
                }
            }
        } catch (Throwable t) {
            logger.error("Load last term failed", t);
        }
    }


    private void persistTerm() {
        try {
            Properties properties  = new Properties();
            properties.put(TERM_PERSIST_KEY_TERM, currTerm);
            properties.put(TERM_PERSIST_KEY_VOTE_FOR, currVoteFor == null ? "" : currVoteFor);
            String data = IOUtils.properties2String(properties);
            IOUtils.string2File(data, dLegerConfig.getDefaultPath() + File.separator + TERM_PERSIST_FILE);
        } catch (Throwable t) {
            logger.error("Persist curr term failed", t);
        }
    }


    public long currTerm(){
        return currTerm;
    }

    public String currVoteFor() {
        return currVoteFor;
    }

    public synchronized void setCurrVoteFor(String currVoteFor) {
        this.currVoteFor = currVoteFor;
        persistTerm();
    }

    public synchronized long nextTerm() {
        assert role.get() == CANDIDATE;
        if (knownMaxTermInGroup > currTerm) {
            currTerm =  knownMaxTermInGroup;
        } else {
            ++currTerm;
        }
        currVoteFor = null;
        persistTerm();
        return currTerm;
    }

    public synchronized void changeToLeader(long term) {
        assert currTerm == term;
        this.role.set(LEADER);
        this.leaderId = selfId;
    }

    public synchronized void changeToFollower(long term, String leaderId) {
        assert currTerm == term;
        this.role.set(FOLLOWER);
        this.leaderId = leaderId;
        syncIndex.clear();
    }

    public synchronized void changeToCandidate(long term) {
        assert term >= currTerm;
        if (term > knownMaxTermInGroup) {
            knownMaxTermInGroup = term;
        }
        //the currTerm should be promoted in handleVote thread
        this.role.set(CANDIDATE);
        syncIndex.clear();
        this.leaderId = null;
    }

    public void updateSelfIndex(Long index) {
        syncIndex.put(selfId, index);
    }

    public void updateIndex(String nodeId, Long index) {
        syncIndex.put(nodeId, index);
    }

    public String getSelfId() {
        return selfId;
    }
    public String getLeaderId() {
        return leaderId;
    }
    public String getGroup() {
        return group;
    }

    public String getSelfAddr() {
        return peerMap.get(selfId);
    }

    public String getLeaderAddr() {
        return peerMap.get(leaderId);
    }

    public String getPeerAddr(String peerId) {
        return peerMap.get(peerId);
    }

    public boolean isLeader() {
        return role.get() == LEADER;
    }
    public boolean isFollower() {
        return role.get() == FOLLOWER;
    }
    public boolean isCandidate() {
        return role.get() == CANDIDATE;
    }

    public boolean checkQuorumIndex(Long index) {
        int size = syncIndex.size();
        int num = 0;
        for (Long value : syncIndex.values()) {
            if (value >= index) {
                num++;
            }
        }
        return num >= ((size/2) + 1);
    }

    public boolean isQuorum(int num) {
        return num >= ((peerSize()/2) + 1);
    }

    public int peerSize() {
        return peerMap.size();
    }

    public Map<String, String> getPeerMap() {
        return peerMap;
    }
    //just for test
    public void setCurrTerm(long currTerm) {
        assert currTerm >= this.currTerm;
        this.currTerm = currTerm;
    }

    public AtomicInteger getRole() {
        return role;
    }

    public ReentrantLock getDefaultLock() {
        return defaultLock;
    }

    public void setDefaultLock(ReentrantLock defaultLock) {
        this.defaultLock = defaultLock;
    }
}
