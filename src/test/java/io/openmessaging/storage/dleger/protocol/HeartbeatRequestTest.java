/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dleger.protocol;

import io.openmessaging.storage.dleger.DLegerServer;
import io.openmessaging.storage.dleger.ServerTestHarness;
import io.openmessaging.storage.dleger.utils.UtilAll;
import java.util.UUID;
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
            Thread.sleep(300);
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), "n3");
            Assert.assertEquals(DLegerResponseCode.UNKNOWN_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.UNEXPECTED_MEMBER.getCode(), leader.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm() - 1);
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.EXPIRED_TERM.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), "n2");
            Assert.assertEquals(DLegerResponseCode.INCONSISTENT_LEADER.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm());
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }

        {
            HeartBeatRequest request = new HeartBeatRequest();
            request.setGroup(group);
            request.setTerm(leader.getMemberState().currTerm() + 1);
            request.setIds(leader.getMemberState().getSelfId(), follower.getMemberState().getSelfId(), leader.getMemberState().getSelfId());
            Assert.assertEquals(DLegerResponseCode.TERM_NOT_READY.getCode(), follower.handleHeartBeat(request).get().getCode());
            Thread.sleep(100);
            Assert.assertEquals(DLegerResponseCode.SUCCESS.getCode(), follower.handleHeartBeat(request).get().getCode());
        }
        dLegerServer0.shutdown();
        dLegerServer1.shutdown();
    }

}

