package org.apache;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.rocketmq.dleger.cmdline.BaseCommand;
import org.apache.rocketmq.dleger.cmdline.BossCommand;
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
        future.complete("123");
        future.whenComplete((x, y) -> {
            System.out.println(x);
            System.out.println(y);
        });
        //future.completeExceptionally(new Exception("xxxx"));
    }

    @Test
    public void testBossCommand() {
        String[] args = new String[] {"append"};
        BossCommand.main(args);
    }
}
