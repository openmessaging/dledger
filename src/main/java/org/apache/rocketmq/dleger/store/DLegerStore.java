package org.apache.rocketmq.dleger.store;

import org.apache.rocketmq.dleger.entry.DLegerEntry;

public abstract class DLegerStore {


    public abstract long appendAsLeader(DLegerEntry entry);


    public abstract long appendAsFollower(DLegerEntry entry, long leaderTerm, String leaderId);

    public abstract DLegerEntry get(Long index);

    public abstract long getCommittedIndex();

    public abstract long getLegerEndTerm();
    public abstract long getLegerEndIndex();

    public void flush() {

    }

    public void startup() {

    }
    public void shutdown() {

    }
}
