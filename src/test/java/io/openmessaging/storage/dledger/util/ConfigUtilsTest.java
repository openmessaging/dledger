package io.openmessaging.storage.dledger.util;

import io.openmessaging.storage.dledger.dledger.DLedgerProxyConfig;
import io.openmessaging.storage.dledger.utils.ConfigUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConfigUtilsTest {

    @Test
    public void TestConfigUtilsInit() throws Exception {
        DLedgerProxyConfig config = ConfigUtils.parseDLedgerProxyConfig("./src/test/resources/config.example.yaml");
        Assertions.assertEquals(3, config.getConfigs().size());
        for (int i = 0; i < config.getConfigs().size(); i++) {
            Assertions.assertEquals("127.0.0.1:10000", config.getConfigs().get(i).getSelfAddress());
            Assertions.assertEquals("g" + i, config.getConfigs().get(i).getGroup());
        }
    }

    @Test
    public void TestConfigUtilsInitErrorConfig() {
        DLedgerProxyConfig config = null;
        try {
            config = ConfigUtils.parseDLedgerProxyConfig("./src/test/resources/config.example.error.yaml");
        } catch (Exception e) {
            Assertions.assertNotNull(e);
            Assertions.assertEquals("DLedgerServers don't have the same port", e.getMessage());
        }
        Assertions.assertNull(config);
    }

}
