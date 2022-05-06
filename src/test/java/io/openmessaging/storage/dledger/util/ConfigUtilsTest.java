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
        DLedgerProxyConfig config = ConfigUtils.parseDLedgerProxyConfig("./config.example.yaml");
        Assertions.assertEquals(3,config.getConfigs().size());
        for (DLedgerConfig dLedgerConfig : config.getConfigs()) {

        }
    }

}
