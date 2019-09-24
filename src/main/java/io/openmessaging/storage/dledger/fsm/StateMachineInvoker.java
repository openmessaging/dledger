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
package io.openmessaging.storage.dledger.fsm;

import io.openmessaging.storage.dledger.ApplyTask;
import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.exception.DLedgerException;
import io.openmessaging.storage.dledger.protocol.AppendEntryResponse;

public interface StateMachineInvoker {
    long getLastAppliedIndex();

    void onCommit();

    boolean onSnapshotSave();

    boolean onSnapshotLoad();

    boolean onRoleChanged(MemberState.Role role);

    void onException(DLedgerException dLedgerException);

    void apply(ApplyTask applyTask);

    void setStateMachine(StateMachine stateMachine);

    void shutdown();

    void start();

    DLedgerLeaderElector.RoleChangeHandler getRoleChangeHandler();

    boolean isPendingFull(long term);

    void checkAbnormalTask(AppendEntryResponse appendEntryResponse);
}
