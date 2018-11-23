package io.openmessaging.storage.dleger;

import io.openmessaging.storage.dleger.protocol.DLegerResponseCode;
import io.openmessaging.storage.dleger.utils.PreConditions;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import io.openmessaging.storage.dleger.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.openmessaging.storage.dleger.MemberState.Role.CANDIDATE;
import static io.openmessaging.storage.dleger.MemberState.Role.FOLLOWER;
import static io.openmessaging.storage.dleger.MemberState.Role.LEADER;

public class MemberState {

    public static Logger logger = LoggerFactory.getLogger(MemberState.class);


    public enum Role {
        UNKNONW,
        CANDIDATE,
        LEADER,
        FOLLOWER;
    }

    public static final String TERM_PERSIST_FILE = "currterm";
    public static final String TERM_PERSIST_KEY_TERM = "currTerm";
    public static final String TERM_PERSIST_KEY_VOTE_FOR = "voteLeader";


    private final ReentrantLock defaultLock = new ReentrantLock();

    public final DLegerConfig dLegerConfig;

    private final String group;
    private final String selfId;
    private final String peers;

    private Role role = CANDIDATE;

    private String leaderId;

    private long currTerm = -1;
    private String currVoteFor;

    private long legerEndIndex = -1;
    private long legerEndTerm = -1;

    private long knownMaxTermInGroup = -1;

    private Map<String, String> peerMap = new HashMap<>();



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
        PreConditions.check(role == CANDIDATE, DLegerResponseCode.ILLEGAL_MEMBER_STATE, "%s != %s", role, CANDIDATE);
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
        PreConditions.check(currTerm == term, DLegerResponseCode.ILLEGAL_MEMBER_STATE, "%d != %d", currTerm, term);
        this.role = LEADER;
        this.leaderId = selfId;
    }

    public synchronized void changeToFollower(long term, String leaderId) {
        PreConditions.check(currTerm == term, DLegerResponseCode.ILLEGAL_MEMBER_STATE, "%d != %d", currTerm, term);
        this.role = FOLLOWER;
        this.leaderId = leaderId;
    }

    public synchronized void changeToCandidate(long term) {
        assert term >= currTerm;
        PreConditions.check(term >= currTerm, DLegerResponseCode.ILLEGAL_MEMBER_STATE, "should %d >= %d", term, currTerm);
        if (term > knownMaxTermInGroup) {
            knownMaxTermInGroup = term;
        }
        //the currTerm should be promoted in handleVote thread
        this.role = CANDIDATE;
        this.leaderId = null;
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
        return role == LEADER;
    }
    public boolean isFollower() {
        return role== FOLLOWER;
    }
    public boolean isCandidate() {
        return role == CANDIDATE;
    }


    public boolean isQuorum(int num) {
        return num >= ((peerSize()/2) + 1);
    }

    public int peerSize() {
        return peerMap.size();
    }

    public boolean isPeerMember(String id) {
        return id != null && peerMap.containsKey(id);
    }

    public Map<String, String> getPeerMap() {
        return peerMap;
    }




    //just for test
    public void setCurrTermForTest(long term) {
        PreConditions.check(term >= currTerm, DLegerResponseCode.ILLEGAL_MEMBER_STATE);
        this.currTerm = term;
    }

    public Role getRole() {
        return role;
    }

    public ReentrantLock getDefaultLock() {
        return defaultLock;
    }

    public void updateLegerIndexAndTerm(long index, long term) {
        this.legerEndIndex = index;
        this.legerEndTerm = term;
    }

    public long getLegerEndIndex() {
        return legerEndIndex;
    }

    public long getLegerEndTerm() {
        return legerEndTerm;
    }
}
