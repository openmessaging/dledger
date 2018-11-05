package org.apache.rocketmq.dleger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.apache.rocketmq.dleger.protocol.AppendEntryRequest;
import org.apache.rocketmq.dleger.protocol.AppendEntryResponse;
import org.apache.rocketmq.dleger.protocol.DLegerResponseCode;
import org.apache.rocketmq.dleger.utils.UtilAll;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class AppendAndPushTest extends ServerTestHarness {



    @Test
    public void testPushCommittedIndex() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n0", DLegerConfig.FILE);
        List<CompletableFuture<AppendEntryResponse>>  futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setRemoteId(dLegerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[256]);
            CompletableFuture<AppendEntryResponse> future = dLegerServer0.handleAppend(appendEntryRequest);
            futures.add(future);
        }
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(-1, dLegerServer0.getdLegerStore().getCommittedIndex());
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n0", DLegerConfig.FILE);
        long start = System.currentTimeMillis();
        while (UtilAll.elapsed(start) < 3000 && dLegerServer1.getdLegerStore().getCommittedIndex() != 9) {
            UtilAll.sleep(100);
        }
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getCommittedIndex());
        Assert.assertEquals(9, dLegerServer1.getdLegerStore().getCommittedIndex());
    }



    @Test
    public void testPushNetworkOffline() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n0", DLegerConfig.FILE);
        List<CompletableFuture<AppendEntryResponse>>  futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setRemoteId(dLegerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[128]);
            CompletableFuture<AppendEntryResponse> future = dLegerServer0.handleAppend(appendEntryRequest);
            futures.add(future);
        }
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getLegerEndIndex());
        Thread.sleep(dLegerServer0.getdLegerConfig().getMaxWaitAckTimeMs() + 100);
        for (int i = 0; i < futures.size(); i++) {
            CompletableFuture<AppendEntryResponse> future = futures.get(i);
            Assert.assertTrue(future.isDone());
            Assert.assertEquals(DLegerResponseCode.WAIT_QUORUM_ACK_TIMEOUT.getCode(), future.get().getCode());
        }

        boolean hasWait = false;
        for (int i = 0; i < dLegerServer0.getdLegerConfig().getMaxPendingRequestsNum(); i++) {
            AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
            appendEntryRequest.setRemoteId(dLegerServer0.getMemberState().getSelfId());
            appendEntryRequest.setBody(new byte[128]);
            CompletableFuture<AppendEntryResponse> future = dLegerServer0.handleAppend(appendEntryRequest);
            if (future.isDone()) {
                Assert.assertEquals(DLegerResponseCode.LEADER_PENDING_FULL.getCode(), future.get().getCode());
                hasWait = true;
                break;
            }
        }
        Assert.assertTrue(hasWait);
    }



    @Test
    public void testPushNetworkNotStable() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());

        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n0", DLegerConfig.FILE);
        AtomicBoolean sendSuccess = new AtomicBoolean(false);
        AppendEntryRequest appendEntryRequest = new AppendEntryRequest();
        appendEntryRequest.setRemoteId(dLegerServer0.getMemberState().getSelfId());
        appendEntryRequest.setBody(new byte[128]);
        CompletableFuture<AppendEntryResponse> future = dLegerServer0.handleAppend(appendEntryRequest);
        future.whenComplete((x, ex) -> {
            sendSuccess.set(true);
        });
        Thread.sleep(500);
        Assert.assertTrue(!sendSuccess.get());
        //start server1
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n0", DLegerConfig.FILE);
        Thread.sleep(1500);
        Assert.assertTrue(sendSuccess.get());
        //shutdown server1
        dLegerServer1.shutdown();
        sendSuccess.set(false);
        future = dLegerServer0.handleAppend(appendEntryRequest);
        future.whenComplete((x, ex) -> {
            sendSuccess.set(true);
        });
        Thread.sleep(500);
        Assert.assertTrue(!sendSuccess.get());
        //restart servre1
        dLegerServer1 = launchServer(group, peers, "n1", "n0", DLegerConfig.FILE);
        Thread.sleep(1500);
        Assert.assertTrue(sendSuccess.get());

        Assert.assertEquals(0, dLegerServer0.getdLegerStore().getLegerBeginIndex());
        Assert.assertEquals(1, dLegerServer0.getdLegerStore().getLegerEndIndex());
        Assert.assertEquals(0, dLegerServer1.getdLegerStore().getLegerBeginIndex());
        Assert.assertEquals(1, dLegerServer1.getdLegerStore().getLegerEndIndex());
    }


    @Test
    public void testPushMissed() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n0", DLegerConfig.FILE);
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n0", DLegerConfig.FILE);
        DLegerServer mockServer1 = spy(dLegerServer1);
        AtomicInteger callNum = new AtomicInteger(0);
        doAnswer( x -> {
            if (callNum.incrementAndGet() % 3 == 0) {
                return new CompletableFuture<>();
            } else {
                return dLegerServer1.handlePush(x.getArgument(0));
            }
        }).when(mockServer1).handlePush(any());
        ((DLegerRpcNettyService) dLegerServer1.getdLegerRpcService()).setdLegerServer(mockServer1);


        for (int i = 0; i < 10; i++) {
            AppendEntryRequest appendEntryRequest =  new AppendEntryRequest();
            appendEntryRequest.setBody(new byte[128]);
            appendEntryRequest.setRemoteId(dLegerServer0.getMemberState().getSelfId());
            AppendEntryResponse appendEntryResponse  = dLegerServer0.handleAppend(appendEntryRequest).get(3, TimeUnit.SECONDS);
            Assert.assertEquals(appendEntryResponse.getCode(), DLegerResponseCode.SUCCESS.getCode());
            Assert.assertEquals(i, appendEntryResponse.getIndex());
        }
        Assert.assertEquals(0, dLegerServer0.getdLegerStore().getLegerBeginIndex());
        Assert.assertEquals(9, dLegerServer0.getdLegerStore().getLegerEndIndex());

        Assert.assertEquals(0, dLegerServer1.getdLegerStore().getLegerBeginIndex());
        Assert.assertEquals(9, dLegerServer1.getdLegerStore().getLegerEndIndex());
    }


      @Test
      public void testPushTruncate() throws Exception {
          String group = UUID.randomUUID().toString();
          String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
          DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n0", DLegerConfig.FILE);
          for (int i = 0; i < 10; i++) {
              DLegerEntry entry = new DLegerEntry();
              entry.setBody(new byte[128]);
              DLegerEntry resEntry = dLegerServer0.getdLegerStore().appendAsLeader(entry);
              Assert.assertEquals(i, resEntry.getIndex());
          }
          Assert.assertEquals(0, dLegerServer0.getdLegerStore().getLegerBeginIndex());
          Assert.assertEquals(9, dLegerServer0.getdLegerStore().getLegerEndIndex());
          List<DLegerEntry> entries = new ArrayList<>();
          for (long i = 0; i < 10; i++) {
              entries.add(dLegerServer0.getdLegerStore().get(i));
          }
          dLegerServer0.shutdown();

          DLegerServer dLegerServer1 = launchServer(group, peers, "n1", "n0", DLegerConfig.FILE);
          for (int i = 0; i < 5; i++) {
              DLegerEntry resEntry = dLegerServer1.getdLegerStore().appendAsFollower(entries.get(i), 0, "n0");
              Assert.assertEquals(i, resEntry.getIndex());
          }
          dLegerServer1.shutdown();

          //change leader from n0 => n1
          dLegerServer1 = launchServer(group, peers, "n1", "n1", DLegerConfig.FILE);
          dLegerServer0 = launchServer(group, peers, "n0", "n1", DLegerConfig.FILE);
          Thread.sleep(1000);
          Assert.assertEquals(0, dLegerServer0.getdLegerStore().getLegerBeginIndex());
          Assert.assertEquals(4, dLegerServer0.getdLegerStore().getLegerEndIndex());
          Assert.assertEquals(0, dLegerServer1.getdLegerStore().getLegerBeginIndex());
          Assert.assertEquals(4, dLegerServer1.getdLegerStore().getLegerEndIndex());
          for (int i = 0; i < 10; i++) {
              AppendEntryRequest request = new AppendEntryRequest();
              request.setRemoteId(dLegerServer1.getMemberState().getSelfId());
              request.setBody(new byte[128]);
              long appendIndex = dLegerServer1.handleAppend(request).get().getIndex();
              Assert.assertEquals(i + 5, appendIndex);
          }
      }
}
