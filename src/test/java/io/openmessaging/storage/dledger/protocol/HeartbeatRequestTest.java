/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.protocol;

import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.ServerTestHarness;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class HeartbeatRequestTest extends ServerTestHarness {

    @Test
    public void testHeartbeat() throws Exception {
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        DLedgerServer dLedgerServer0 = launchServer(group, peers, "n0");
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1");
        DLedgerServer leader, follower;
        {
            long start = System.currentTimeMillis();
            while (!dLedgerServer0.getMemberState().isLeader() && !dLedgerServer1.getMemberState().isLeader() && DLedgerUtils.elapsed(start) < 3000) {
                Thread.sleep(100);
            }
            Assert.assertTrue(dLedgerServer0.getMemberState().isLeader() || dLedgerServer1.getMemberState().isLeader());
            if (dLedgerServer0.getMemberState().isLeader()) {
                leader = dLedgerServer0;
                follower = dLedgerServer1;
            } else {
                leader = dLedgerServer1;
                follower = dLedgerServer0;
            }
            Thread.sleep(300);
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), "n3");
            Assert.assertEquals(DLedgerResponseCode.UNKNOWN_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm() - 1);
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.EXPIRED_TERM.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), "n2");
            Assert.assertEquals(DLedgerResponseCode.INCONSISTENT_LEADER.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm() + 1);
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.TERM_NOT_READY.getCode(), follower.handleHeartBeat(request).get().getCode());
            Thread.sleep(100);
            Assert.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }
        dLedgerServer0.shutdown();
        dLedgerServer1.shutdown();
    }

    @Test
    public void testIllegalMemberState() throws Exception {
        long start;
        String group = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d;n1-localhost:%d;n2-localhost:%d", nextPort(), nextPort(), nextPort());
        String preferredLeaderId = "n0";
        DLedgerServer dLedgerServer0;
        DLedgerServer dLedgerServer1 = launchServer(group, peers, "n1", preferredLeaderId);
        DLedgerServer dLedgerServer2 = launchServer(group, peers, "n2", preferredLeaderId);

        DLedgerServer leader, follower, preferredLeader;
        {
            start = System.currentTimeMillis();
            while (!dLedgerServer1.getMemberState().isLeader() && !dLedgerServer2.getMemberState().isLeader()) {
                Thread.sleep(100);
                if (DLedgerUtils.elapsed(start) > 1000 * 10) {
                    break;
                }
            }

            Assert.assertTrue(dLedgerServer1.getMemberState().isLeader() || dLedgerServer2.getMemberState().isLeader());

            if (dLedgerServer1.getMemberState().isLeader()) {
                leader = dLedgerServer1;
                follower = dLedgerServer2;
            } else {
                leader = dLedgerServer2;
                follower = dLedgerServer1;
            }

            Thread.sleep(500);
        }

        Assert.assertTrue(leader.getMemberState().isLeader());
        Assert.assertTrue(follower.getMemberState().isFollower());

        {
            long ledgerEndIndex = leader.getMemberState().getLedgerEndIndex();
            long term = leader.getMemberState().currTerm();
            leader.getMemberState().updateLedgerIndexAndTerm( 999, term);
            follower.getMemberState().updateLedgerIndexAndTerm( 996, term);
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            dLedgerServer0 = launchServer(group, peers, "n0", preferredLeaderId);
            long term = dLedgerServer0.getMemberState().currTerm();
            dLedgerServer0.getMemberState().updateLedgerIndexAndTerm( 357, term);

            start = System.currentTimeMillis();
            while (!dLedgerServer0.getMemberState().isFollower() || dLedgerServer0.getMemberState().currTerm() != leader.getMemberState().currTerm()) {
                Thread.sleep(100);
                if (DLedgerUtils.elapsed(start) > 1000 * 30) {
                    break;
                }
            }

            Assert.assertTrue(dLedgerServer0.getMemberState().isFollower());
            preferredLeader = dLedgerServer0;
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), preferredLeader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), preferredLeader.handleHeartBeat(request).get().getCode());
        }

        {
            LeadershipTransferRequest request = new LeadershipTransferRequest();
            MemberState memberState = leader.getMemberState();
            request.setGroup(memberState.getGroup());
            request.setLeaderId(memberState.getLeaderId());
            request.setLocalId(memberState.getSelfId());
            request.setRemoteId(preferredLeaderId);
            request.setTerm(memberState.currTerm());
            request.setTakeLeadershipLedgerIndex(memberState.getLedgerEndIndex());
            request.setTransferId(memberState.getSelfId());
            request.setTransferId(preferredLeaderId);
            preferredLeader.handleLeadershipTransfer(request);
            start = System.currentTimeMillis();
            while (!preferredLeader.getMemberState().isCandidate()) {
                Thread.sleep(100);
                if (DLedgerUtils.elapsed(start) > 1000 * 10) {
                    break;
                }
            }
            Thread.sleep(500);
        }

        Assert.assertTrue(preferredLeader.getMemberState().isCandidate());
        Assert.assertTrue(preferredLeader.getMemberState().currTerm() > leader.getMemberState().currTerm());
        Assert.assertTrue(preferredLeader.getMemberState().getLedgerEndIndex() < leader.getMemberState().getLedgerEndIndex());
        Assert.assertTrue(preferredLeader.getMemberState().currTerm() > follower.getMemberState().currTerm());
        Assert.assertTrue(preferredLeader.getMemberState().getLedgerEndIndex() < follower.getMemberState().getLedgerEndIndex());


        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), preferredLeader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.ILLEGAL_MEMBER_STATE.getCode(), preferredLeader.handleHeartBeat(request).get().getCode());
        }

        Assert.assertTrue(preferredLeader.getMemberState().isFollower());
        Assert.assertTrue(preferredLeader.getMemberState().currTerm() == leader.getMemberState().currTerm());
        Assert.assertTrue(preferredLeader.getMemberState().currVoteFor() == leader.getMemberState().getSelfId());

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), preferredLeader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), preferredLeader.handleHeartBeat(request).get().getCode());
        }

        dLedgerServer0.shutdown();
        dLedgerServer1.shutdown();
        dLedgerServer2.shutdown();
    }
}

