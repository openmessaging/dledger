package org.apache.rocketmq.dleger.entry;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.dleger.util.FileTestUtil;
import org.junit.After;

public class ServerTestBase {


    protected List<String> bases = new ArrayList<>();


    @After
    public synchronized void shutdown() {
        for (String base: bases) {
            try {
                FileTestUtil.deleteFile(base);
            } catch (Throwable ignored) {

            }
        }
    }
}
