package com.couchbase.client.dcp.state;

import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

import java.util.Collections;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;


public class SessionStateRollbackToPositionTest {

    @Test
    public void rollbackToPosition() throws Exception {
        // Populate a session state with dummy data for a single partition
        final SessionState sessionState = new SessionState();
        PartitionState partitionState = new PartitionState();
        partitionState.addToFailoverLog(1, 345);
        partitionState.addToFailoverLog(5, 12345);
        partitionState.addToFailoverLog(-1L, 4567); // seqnos are unsigned, so -1 is max value
        partitionState.setStartSeqno(1);
        partitionState.setEndSeqno(1000);
        partitionState.setSnapshotStartSeqno(2);
        partitionState.setSnapshotEndSeqno(3);
        sessionState.set(0, partitionState);

        sessionState.rollbackToPosition((short) 0, 1L);
        assertEquals(1, partitionState.getStartSeqno());
        assertEquals(1, partitionState.getSnapshotStartSeqno());
        assertEquals(1, partitionState.getSnapshotEndSeqno());
        assertEquals(singletonList(new FailoverLogEntry(1, 345)), partitionState.getFailoverLog());
    }

}

