package org.apache;

import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.dleger.cmdline.BossCommand;
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
}
