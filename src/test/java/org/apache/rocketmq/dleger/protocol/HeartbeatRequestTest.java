package org.apache.rocketmq.dleger.protocol;

import java.util.UUID;
import org.apache.rocketmq.dleger.DLegerServer;
import org.apache.rocketmq.dleger.ServerTestHarness;
import org.apache.rocketmq.dleger.utils.UtilAll;
import org.junit.Assert;
import org.junit.Test;

public class HeartbeatRequestTest extends ServerTestHarness {

    @Test
    public void testHeartbeat() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLegerServer dLegerServer0 = launchServer(group, peers, "n0");
        DLegerServer dLegerServer1 = launchServer(group, peers, "n1");
        DLegerServer leader, follower;
        {
            long start = System.currentTimeMillis();
            while (!dLegerServer0.getMemberState().isLeader() && !dLegerServer1.getMemberState().isLeader() && UtilAll.elapsed(start) < 3000) {
                Thread.sleep(100);
            }
            Assert.assertTrue(dLegerServer0.getMemberState().isLeader() || dLegerServer1.getMemberState().isLeader());
            if (dLegerServer0.getMemberState().isLeader()) {
                leader = dLegerServer0;
                follower = dLegerServer1;
            } else {
                leader = dLegerServer1;
                follower = dLegerServer0;
            }
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), "n3");
            Assert.assertEquals(DLegerResponseCode.UNKNOWN_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.UNEXPECTED_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setTerm(leader.getMemberState().currTerm() - 1);
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.EXPIRED_TERM.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), "n2");
            Assert.assertEquals(DLegerResponseCode.INCONSISTENT_LEADER.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setTerm(leader.getMemberState().currTerm() + 1);
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.TERM_NOT_READY.getCode(), follower.handleHeartBeat(request).get().getCode());
            Thread.sleep(100);
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }
        dLegerServer0.shutdown();
        dLegerServer1.shutdown();
    }

}

