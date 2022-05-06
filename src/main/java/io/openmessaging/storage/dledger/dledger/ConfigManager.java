package io.openmessaging.storage.dledger.dledger;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.cmdline.ConfigCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author TheR1sing3un
 * @date 2022/5/3 22:08
 * @description
 */

public class ConfigManager {

    private static Logger logger = LoggerFactory.getLogger(ConfigManager.class);

    private DLedgerProxyConfig dLedgerProxyConfig;

    //selfId -> DLedgerConfig
    private HashMap<String, DLedgerConfig> configMap;

    //selfId -> address
    private HashMap<String,String> addressMap;


    public ConfigManager(final DLedgerProxyConfig dLedgerProxyConfig){
        this.dLedgerProxyConfig = dLedgerProxyConfig;
        initConfig(dLedgerProxyConfig);
    }

    public DLedgerProxyConfig getdLedgerProxyConfig() {
        return dLedgerProxyConfig;
    }

    public void setdLedgerProxyConfig(DLedgerProxyConfig dLedgerProxyConfig) {
        this.dLedgerProxyConfig = dLedgerProxyConfig;
    }

    private void initConfig(DLedgerProxyConfig dLedgerProxyConfig){
        this.configMap = new HashMap<>();
        this.addressMap = new HashMap<>();
        for (DLedgerConfig config : this.dLedgerProxyConfig.getConfigs()) {
            this.configMap.put(config.getSelfId(), config);
            this.addressMap.putAll(config.getPeerAddressMap());
        }
    }

    public String getAddress(String selfId){
        return this.addressMap.get(selfId);
    }
}
