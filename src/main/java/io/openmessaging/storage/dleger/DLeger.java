package io.openmessaging.storage.dleger;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.JCommander;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DLeger {

    private static Logger logger = LoggerFactory.getLogger(DLeger.class);
    public static void main(String args[]) {
        DLegerConfig dLegerConfig = new DLegerConfig();
        JCommander.Builder builder = JCommander.newBuilder().addObject(dLegerConfig);
        JCommander jc = builder.build();
        jc.parse(args);
        DLegerServer dLegerServer = new DLegerServer(dLegerConfig);
        dLegerServer.startup();
        logger.info("[{}] group {} start ok with config {}", dLegerConfig.getSelfId(), dLegerConfig.getGroup(), JSON.toJSONString(dLegerConfig));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;
            private AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    logger.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        dLegerServer.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        logger.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        }, "ShutdownHook"));
    }
}
