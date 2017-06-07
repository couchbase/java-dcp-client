package com.couchbase.client.dcp.state.json;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class SessionStateSerializerTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void serialization() throws Exception {
        // Populate a session state with dummy data for a single partition
        SessionState sessionState = new SessionState();
        PartitionState partitionState = new PartitionState();
        partitionState.addToFailoverLog(5, 12345);
        partitionState.setStartSeqno(1);
        partitionState.setEndSeqno(1000);
        partitionState.setSnapshotStartSeqno(2);
        partitionState.setSnapshotEndSeqno(3);
        sessionState.set(0, partitionState);

        byte[] actualJson = sessionState.export(StateFormat.JSON);
        String expectedJson =
                "{\"v\":1,\"ps\":[{\"flog\":[{\"seqno\":5,\"uuid\":12345}],\"ss\":1,\"es\":1000,\"sss\":2,\"ses\":3}]}";

        assertJsonEquals(expectedJson, actualJson);
    }

    /**
     * Asserts the actual JSON is semantically equivalent to the expected JSON
     * (ignores differences in property order, whitespace between tokens, etc.)
     */
    private static void assertJsonEquals(String expected, byte[] actual) throws IOException {
        JsonNode jsonNode = objectMapper.readTree(actual);
        JsonNode expectedJsonNode = objectMapper.readTree(expected);
        assertEquals(expectedJsonNode, jsonNode);
    }
}