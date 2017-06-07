package com.couchbase.client.dcp.state.json;

import static org.junit.Assert.assertEquals;

import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import org.junit.Test;

public class SessionStateDeserializerTest {
    @Test
    public void deserialize() throws Exception {
        SessionState sessionState = new SessionState();

        String json = "{\"v\":1,\"ps\":[{\"flog\":[{\"seqno\":5,\"uuid\":12345}],\"ss\":1,\"es\":1000,\"sss\":2,\"ses\":3}]}";
        sessionState.setFromJson(json.getBytes("UTF-8"));

        PartitionState partitionState = sessionState.get(0);

        assertEquals(1, partitionState.getFailoverLog().size());
        FailoverLogEntry failoverLogEntry = partitionState.getFailoverLog().get(0);
        assertEquals(5, failoverLogEntry.getSeqno());
        assertEquals(12345, failoverLogEntry.getUuid());

        assertEquals(1, partitionState.getStartSeqno());
        assertEquals(1000, partitionState.getEndSeqno());
        assertEquals(2, partitionState.getSnapshotStartSeqno());
        assertEquals(3, partitionState.getSnapshotEndSeqno());
    }
}