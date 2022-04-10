package io.openmessaging.storage.dledger.statemachine;

import org.junit.jupiter.api.Test;

import io.openmessaging.storage.dledger.DLedgerConfig;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;
import io.openmessaging.storage.dledger.store.DLedgerMemoryStore;
import io.openmessaging.storage.dledger.utils.Pair;

import static org.junit.jupiter.api.Assertions.*;

class StateMachineCallerTest {


    public Pair<StateMachineCaller, MockStateMachine> mockCaller() {
        DLedgerConfig config = new DLedgerConfig();
        MemberState memberState = new MemberState(config);
        memberState.changeToLeader(0);
        final DLedgerMemoryStore dLedgerMemoryStore = new DLedgerMemoryStore(config, memberState);
        for (int i = 0; i < 10; i++) {
            final DLedgerEntry entry = new DLedgerEntry();
            entry.setIndex(i);
            entry.setTerm(0);
            dLedgerMemoryStore.appendAsLeader(entry);
        }
        final MockStateMachine fsm = new MockStateMachine();
        final StateMachineCaller caller = new StateMachineCaller(dLedgerMemoryStore, fsm);
        caller.start();
        return new Pair<>(caller, fsm);
    }


    @Test
    public void testOnCommitted() throws InterruptedException {
        final Pair<StateMachineCaller, MockStateMachine> result = mockCaller();
        final StateMachineCaller caller = result.getKey();
        final MockStateMachine fsm = result.getValue();
        caller.onCommitted(9);
        Thread.sleep(1000);
        assertEquals(fsm.getAppliedIndex(), 9);
        assertEquals(fsm.getTotalEntries(), 10);

        caller.shutdown();
    }
}