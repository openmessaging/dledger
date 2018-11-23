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

package io.openmessaging.storage.dleger.store;

import io.openmessaging.storage.dleger.MemberState;
import io.openmessaging.storage.dleger.entry.DLegerEntry;

public abstract class DLegerStore {

    public MemberState getMemberState() {
        return null;
    }

    public abstract DLegerEntry appendAsLeader(DLegerEntry entry);

    public abstract DLegerEntry appendAsFollower(DLegerEntry entry, long leaderTerm, String leaderId);

    public abstract DLegerEntry get(Long index);

    public abstract long getCommittedIndex();

    public void updateCommittedIndex(long term, long committedIndex) {

    }

    public abstract long getLegerEndTerm();

    public abstract long getLegerEndIndex();

    public abstract long getLegerBeginIndex();

    protected void updateLegerEndIndexAndTerm() {
        if (getMemberState() != null) {
            getMemberState().updateLegerIndexAndTerm(getLegerEndIndex(), getLegerEndTerm());
        }
    }

    public void flush() {

    }

    public long truncate(DLegerEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    public void startup() {

    }

    public void shutdown() {

    }
}
