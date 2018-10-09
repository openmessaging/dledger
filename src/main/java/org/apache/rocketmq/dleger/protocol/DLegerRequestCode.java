package org.apache.rocketmq.dleger.protocol;

public class DLegerRequestCode {


    //the client will use it
    public static final int APPEND = 50001;
    public static final int GET = 50002;

    //the peer will use it
    public static final int VOTE = 51001;
    public static final int HEART_BEAT = 51002;
    public static final int PULL = 51003;
    public static final int PUSH = 51004;
}
