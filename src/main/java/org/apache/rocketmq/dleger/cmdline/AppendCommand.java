package org.apache.rocketmq.dleger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import org.apache.rocketmq.dleger.client.DLegerClient;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendCommand extends BaseCommand{

    private static Logger logger = LoggerFactory.getLogger(AppendCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";


    @Parameter(names = {"--data", "-d"}, description = "the data to append")
    private String data = "Hello";

    @Parameter(names = {"--count", "-c"}, description = "append several times")
    private int count = 1;

    @Override
    public void doCommand()  {
        DLegerClient dLegerClient = new DLegerClient(group, peers);
        dLegerClient.startup();
        for (int i = 0; i < count; i++) {
            AppendEntryResponse response = dLegerClient.append(data.getBytes());
            logger.info("Append Result:{}", JSON.toJSONString(response));
        }
        dLegerClient.shutdown();
    }
}
