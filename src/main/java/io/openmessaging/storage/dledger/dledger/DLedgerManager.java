package io.openmessaging.storage.dledger.dledger;

import io.netty.util.internal.ReadOnlyIterator;
import io.openmessaging.storage.dledger.DLedger;
import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerRpcService;
import io.openmessaging.storage.dledger.DLedgerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * @author TheR1sing3un
 * @date 2022/5/3 22:07
 * @description
 */

public class DLedgerManager {

    private static Logger logger = LoggerFactory.getLogger(DLedgerManager.class);

    private ConcurrentHashMap<String, Map<String, DLedgerServer>> servers;

    public DLedgerManager(DLedgerProxyConfig dLedgerProxyConfig, DLedgerRpcService dLedgerRpcService) {
        this.servers = new ConcurrentHashMap<>();
        initDLedgerServer(dLedgerProxyConfig, dLedgerRpcService);
    }

    private void initDLedgerServer(DLedgerProxyConfig dLedgerProxyConfig, DLedgerRpcService dLedgerRpcService){
        for (DLedgerConfig config : dLedgerProxyConfig.getConfigs()) {
            DLedgerServer server = new DLedgerServer(config);
            server.registerDLedgerRpcService(dLedgerRpcService);
            if (!servers.containsKey(config.getGroup())){
                servers.put(config.getGroup(), new ConcurrentHashMap<>());
            }
            servers.get(config.getGroup()).put(config.getSelfId(), server);
        }
    }

    public DLedgerServer getDLedgerServer(String groupId, String selfId){
        return this.servers.containsKey(groupId) ? this.servers.get(groupId).get(selfId) : null;
    }

    public void startup(){
        Iterator<Map.Entry<String, Map<String, DLedgerServer>>> iteratorServerList = servers.entrySet().iterator();
        while(iteratorServerList.hasNext()){
            Map.Entry<String, Map<String, DLedgerServer>> next = iteratorServerList.next();
            Iterator<Map.Entry<String, DLedgerServer>> iteratorServer = next.getValue().entrySet().iterator();
            while(iteratorServer.hasNext()){
                DLedgerServer server = iteratorServer.next().getValue();
                server.startup();
            }
        }
    }

    public void shutdown() {
        Iterator<Map.Entry<String, Map<String, DLedgerServer>>> iteratorServerList = servers.entrySet().iterator();
        while(iteratorServerList.hasNext()){
            Map.Entry<String, Map<String, DLedgerServer>> next = iteratorServerList.next();
            Iterator<Map.Entry<String, DLedgerServer>> iteratorServer = next.getValue().entrySet().iterator();
            while(iteratorServer.hasNext()){
                DLedgerServer server = iteratorServer.next().getValue();
                server.shutdown();
            }
        }
    }

    public List<DLedgerServer> getDLedgerServers(){
        List<DLedgerServer> serverList = new ArrayList<DLedgerServer>();
        Iterator<Map.Entry<String, Map<String, DLedgerServer>>> iteratorServerList = servers.entrySet().iterator();
        while(iteratorServerList.hasNext()){
            Map.Entry<String, Map<String, DLedgerServer>> next = iteratorServerList.next();
            Iterator<Map.Entry<String, DLedgerServer>> iteratorServer = next.getValue().entrySet().iterator();
            while(iteratorServer.hasNext()){
                DLedgerServer server = iteratorServer.next().getValue();
                serverList.add(server);
            }
        }
        return serverList;
    }

    public void setRPCService(DLedgerRpcService service){
        Iterator<Map.Entry<String, Map<String, DLedgerServer>>> iteratorServerList = servers.entrySet().iterator();
        while(iteratorServerList.hasNext()){
            Map.Entry<String, Map<String, DLedgerServer>> next = iteratorServerList.next();
            Iterator<Map.Entry<String, DLedgerServer>> iteratorServer = next.getValue().entrySet().iterator();
            while(iteratorServer.hasNext()){
                DLedgerServer server = iteratorServer.next().getValue();

            }
        }
    }
}
