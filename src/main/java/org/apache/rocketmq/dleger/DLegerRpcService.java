package org.apache.rocketmq.dleger;

import org.apache.rocketmq.dleger.protocol.DLegerProtocol;
import org.apache.rocketmq.dleger.protocol.DLegerProtocolHander;

public abstract class DLegerRpcService implements DLegerProtocol, DLegerProtocolHander {



    public abstract void startup();
    public abstract void shutdown();

}
