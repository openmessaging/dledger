package io.openmessaging.storage.dledger.util;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.dledger.DLedgerProxyConfig;
import io.openmessaging.storage.dledger.utils.ConfigUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author TheR1sing3un
 * @date 2022/5/3 23:02
 * @description
 */

public class ConfigUtilsTest {

    @Test
    public void TestConfigUtilsInit() throws IOException {
        DLedgerProxyConfig config = ConfigUtils.parseDLedgerProxyConfig("./src/test/resources/config.example.yaml");
        Assertions.assertEquals(3, config.getConfigs().size());
        for (int i = 0; i < config.getConfigs().size(); i++) {
            Assertions.assertEquals("127.0.0.1:10000", config.getConfigs().get(i).getSelfAddress());
            Assertions.assertEquals("g" + i, config.getConfigs().get(i).getGroup());
        }
    }

}
