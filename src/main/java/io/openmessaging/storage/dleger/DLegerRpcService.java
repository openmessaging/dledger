package io.openmessaging.storage.dleger;

import io.openmessaging.storage.dleger.protocol.DLegerProtocolHander;
import io.openmessaging.storage.dleger.protocol.DLegerProtocol;

public abstract class DLegerRpcService implements DLegerProtocol, DLegerProtocolHander {



    public abstract void startup();
    public abstract void shutdown();

}
