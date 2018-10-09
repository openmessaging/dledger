package org.apache.rocketmq.dleger;

import com.beust.jcommander.JCommander;
import org.junit.Assert;
import org.junit.Test;

public class DLegerConfigTest {


    @Test
    public void testParseArguments() {
        DLegerConfig dLegerConfig = new DLegerConfig();
        JCommander.Builder builder = JCommander.newBuilder().addObject(dLegerConfig);
        JCommander jc = builder.build();
        jc.parse("-i", "n1", "-g", "test", "-p", "n1-localhost:21911", "-s", "/tmp");
        Assert.assertEquals("n1", dLegerConfig.getSelfId());
        Assert.assertEquals("test", dLegerConfig.getGroup());
        Assert.assertEquals("n1-localhost:21911", dLegerConfig.getPeers());
        Assert.assertEquals("/tmp", dLegerConfig.getStoreBaseDir());
        System.out.println(jc.getParsedCommand());
    }
}
