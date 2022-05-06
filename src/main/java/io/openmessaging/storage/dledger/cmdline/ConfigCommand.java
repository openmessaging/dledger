package io.openmessaging.storage.dledger.cmdline;

import com.beust.jcommander.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfigCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(ConfigCommand.class);

    @Parameter(names = {"--config", "-c"}, description = "Config path of DLedger")
    private String config = "./config.yaml";

    @Override
    public void doCommand() {

    }

    public String getConfigPath(){
        return config;
    }
}
