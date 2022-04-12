/*
 * Copyright 2017-2022 The DLedger Authors.
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
import io.openmessaging.storage.dledger.ServerTestHarness;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
            Assertions.assertTrue(dLedgerServer0.getMemberState().isLeader() || dLedgerServer1.getMemberState().isLeader());
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
            Assertions.assertEquals(DLedgerResponseCode.UNKNOWN_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assertions.assertEquals(DLedgerResponseCode.UNEXPECTED_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm() - 1);
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assertions.assertEquals(DLedgerResponseCode.EXPIRED_TERM.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), "n2");
            Assertions.assertEquals(DLedgerResponseCode.INCONSISTENT_LEADER.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm() + 1);
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assertions.assertEquals(DLedgerResponseCode.TERM_NOT_READY.getCode(), follower.handleHeartBeat(request).get().getCode());
            Thread.sleep(100);
            Assertions.assertEquals(DLedgerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }
        dLedgerServer0.shutdown();
        dLedgerServer1.shutdown();
    }

}

