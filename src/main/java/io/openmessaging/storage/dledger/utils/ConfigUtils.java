package io.openmessaging.storage.dledger.utils;

import io.openmessaging.storage.dledger.dledger.DLedgerProxyConfig;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;

public class ConfigUtils {

    public static DLedgerProxyConfig parseDLedgerProxyConfig(String path) throws IOException {
        DLedgerProxyConfig config = null;
        Yaml yaml = new Yaml(new Constructor(DLedgerProxyConfig.class));
        InputStream inputStream = ConfigUtils.class.getClassLoader().getResourceAsStream(path);
        config = yaml.load(inputStream);
        inputStream.close();
        return config;
    }

}
