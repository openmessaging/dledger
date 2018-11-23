package io.openmessaging.storage.dleger.cmdline;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.Parameter;
import io.openmessaging.storage.dleger.client.DLegerClient;
import io.openmessaging.storage.dleger.entry.DLegerEntry;
import io.openmessaging.storage.dleger.protocol.GetEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetCommand extends BaseCommand {

    private static Logger logger = LoggerFactory.getLogger(GetCommand.class);

    @Parameter(names = {"--group", "-g"}, description = "Group of this server")
    private String group = "default";

    @Parameter(names = {"--peers", "-p"}, description = "Peer info of this server")
    private String peers = "n0-localhost:20911";


    @Parameter(names = {"--index", "-i"}, description = "get entry from index")
    private long index = 0;


    @Override
    public void doCommand() {
        DLegerClient dLegerClient = new DLegerClient(group, peers);
        dLegerClient.startup();
        GetEntriesResponse response = dLegerClient.get(index);
        logger.info("Get Result:{}", JSON.toJSONString(response));
        if (response.getEntries() != null && response.getEntries().size() > 0) {
            for (DLegerEntry entry: response.getEntries()) {
                logger.info("Get Result index:{} {}", entry.getIndex(), new String(entry.getBody()));
            }
        }
        dLegerClient.shutdown();
    }
}
