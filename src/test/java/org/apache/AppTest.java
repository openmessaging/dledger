package org.apache;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.swing.SizeRequirements;
import org.apache.rocketmq.dleger.cmdline.BossCommand;
import org.apache.rocketmq.dleger.utils.UtilAll;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Unit test for simple App.
 */
public class AppTest 
{

    public String copy(String a) {
        System.out.println("XXX");
        return a;
    }
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        System.out.println(true);
        assertTrue( true );
    }

    @Test
    public void testLatch() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        latch.countDown();
        latch.countDown();
        latch.countDown();
        System.out.println(latch.getCount());
        latch.await(3, TimeUnit.SECONDS);
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
    public void testFormat() {
        long start = System.currentTimeMillis();

        for (int i = 0; i < 1000000; i++) {
            String message = String.format("pos: %d size: %d magic:%d index:%d term:%d", 0, 0, 0, 0, 0);
        }
        System.out.println(UtilAll.elapsed(start));

    }

    @Test
    public void testBossCommand() {
        String[] args = new String[] {"append"};
        BossCommand.main(args);
    }

    @Test
    public void testMock() {
        AppTest test = new AppTest();
        AppTest mock = spy(AppTest.class);
        //when(mock.copy(anyString())).thenReturn("123");
        //System.out.println(mock.copy("111"));
       /* when(mock.copy(anyString())).thenAnswer((x) -> {
            System.out.println(x.getArgument(0).toString());
            return "ans";
        });*/
        doAnswer((x) -> {
            System.out.println(x.getArgument(0).toString());
            return "ans";
        }).when(mock).copy(anyString());
        System.out.println(mock.copy("111"));
        System.out.println(mock.copy("111"));
        System.out.println(mock.copy("111"));
    }

    @Test
    public void testConcurrent() throws Exception {
        ConcurrentMap<Integer, String> concurrentMap = new ConcurrentHashMap<>();
        for (int i = 0; i < 100; i++) {
            concurrentMap.putIfAbsent(i, "value" + i);
        }
        Thread t1 = new Thread(new Runnable() {
            @Override public void run() {
               for (Integer key: concurrentMap.keySet()) {
                   if (key % 3 == 0) {
                       System.out.println(System.currentTimeMillis() + ":" + Thread.currentThread().getId() + " delete " + key);
                       concurrentMap.remove(key);
                   } else {
                       System.out.println(System.currentTimeMillis() + ":" + Thread.currentThread().getId() + " display " + key);
                   }
                   try {
                       Thread.sleep(3);
                   } catch (Exception ignored) {

                   }
               }
            }
        });

        Thread t2 = new Thread(new Runnable() {
            @Override public void run() {
                for (Integer key: concurrentMap.keySet()) {
                    if (key % 5 == 0) {
                        System.out.println(System.currentTimeMillis() + ":" + Thread.currentThread().getId() + " delete " + key);
                        concurrentMap.remove(key);
                    } else {
                        System.out.println(System.currentTimeMillis() + ":" + Thread.currentThread().getId() + " display " + key);
                    }
                    try {
                        Thread.sleep(3);
                    } catch (Exception ignored) {

                    }
                }
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

    }
}
