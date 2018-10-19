package org.apache.rocketmq.dleger.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerConfig;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.entry.DLegerEntry;
import org.junit.Assert;
import org.junit.Test;

public class AppendAndPushTest extends ServerTestHarness {

      @Test
      public void testTruncate() throws Exception {
          String group = UUID.randomUUID().toString();
          String peers = String.format("n0-localhost:%d;n1-localhost:%d", nextPort(), nextPort());
          DLegerServer dLegerServer0 = launchServer(group, peers, "n0", "n0", DLegerConfig.FILE);
          for (int i = 0; i < 10; i++) {
              DLegerEntry entry = new DLegerEntry();
              entry.setBody(new byte[128]);
              long appendIndex = dLegerServer0.getdLegerStore().appendAsLeader(entry);
              Assert.assertEquals(i, appendIndex);
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
              long followerIndex = dLegerServer1.getdLegerStore().appendAsFollower(entries.get(i), 0, "n0");
              Assert.assertEquals(i, followerIndex);
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
              request.setBody(new byte[128]);
              long appendIndex = dLegerServer1.handleAppend(request).get().getIndex();
              Assert.assertEquals(i + 5, appendIndex);
          }
      }
}
