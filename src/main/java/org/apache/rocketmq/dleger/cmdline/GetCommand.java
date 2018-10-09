package org.apache.rocketmq.dleger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import org.apache.rocketmq.dleger.client.DLegerClient;
import org.apache.rocketmq.dleger.protocol.GetEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(GetCommand.class);

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";


    @Parameter(names = {"--index", "-i"}, description = "get entry from index")
    private long index = 0;


    @Override
    public void doCommand() {
        DLegerClient dLegerClient = new DLegerClient(peers);
        dLegerClient.startup();
        GetEntriesResponse response = dLegerClient.get(index);
        logger.info("Append Result:{}", JSON.toJSONString(response));
        dLegerClient.shutdown();
    }
}
