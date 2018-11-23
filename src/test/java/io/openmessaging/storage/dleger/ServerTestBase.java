package io.openmessaging.storage.dleger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import io.openmessaging.storage.dleger.util.FileTestUtil;
import org.junit.After;

public class ServerTestBase {


    private static final AtomicInteger PORT_COUNTER = new AtomicInteger(30000);
    protected List<String> bases = new ArrayList<>();

    public static int nextPort() {
        return PORT_COUNTER.incrementAndGet();
    }

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
