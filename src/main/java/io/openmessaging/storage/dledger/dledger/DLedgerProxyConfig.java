package io.openmessaging.storage.dledger.dledger;


import io.openmessaging.storage.dledger.DLedgerConfig;

import java.util.List;

public class DLedgerProxyConfig {

    private List<DLedgerConfig> configs;

    public List<DLedgerConfig> getConfigs() {
        return configs;
    }

    public void setConfigs(List<DLedgerConfig> configs) {
        this.configs = configs;
    }

    @Override
    public String toString() {
        return "DLedgerProxyConfig{" +
                "configs=" + configs +
                '}';
    }
}
