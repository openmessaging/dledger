package org.apache;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import org.apache.rocketmq.dleger.cmdline.BaseCommand;
import org.apache.rocketmq.dleger.cmdline.BossCommand;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.utils.PreConditions;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue( true );
    }

    @Test
    public void testFuture() throws Exception {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.whenComplete((x, y) -> {
            System.out.println(x);
            System.out.println(y);
            System.out.println(x.equals(y));
        }).whenComplete((x, y) -> {
            System.out.println(x);
            System.out.println(y);
        });
        future.complete("123");
    }

    @Test
    public void testBossCommand() {
        String[] args = new String[] {"append"};
        BossCommand.main(args);
    }
}
