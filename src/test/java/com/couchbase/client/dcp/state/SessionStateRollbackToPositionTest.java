package com.couchbase.client.dcp.state;

import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;


public class SessionStateRollbackToPositionTest {

    @Test
    public void rollbackToPosition() throws Exception {
        // Populate a session state with dummy data for a single partition
        final SessionState sessionState = new SessionState();
        PartitionState partitionState = new PartitionState();
        partitionState.addToFailoverLog(5, 12345);
        partitionState.setStartSeqno(1);
        partitionState.setEndSeqno(1000);
        partitionState.setSnapshotStartSeqno(2);
        partitionState.setSnapshotEndSeqno(3);
        sessionState.set(0, partitionState);

        Throwable th = catchThrowable(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                sessionState.rollbackToPosition((short) 0, 1L);
            }
        });

        assertThat(th).isNull();
    }

}

