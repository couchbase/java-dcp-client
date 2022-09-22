/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.state.json;

import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SessionStateSerializerTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void serialization() throws Exception {
    // Populate a session state with dummy data for a single partition
    SessionState sessionState = new SessionState();
    PartitionState partitionState = new PartitionState();
    partitionState.setFailoverLog(singletonList(
        new FailoverLogEntry(5, 12345)));
    partitionState.setStartSeqno(1);
    partitionState.setEndSeqno(1000);
    partitionState.setSnapshot(new SnapshotMarker(2, 3));
    sessionState.set(0, partitionState);

    byte[] actualJson = sessionState.export(StateFormat.JSON);
    String expectedJson =
        "{\"v\":1,\"ps\":[{\"flog\":[{\"seqno\":5,\"uuid\":12345}],\"ss\":1,\"es\":1000,\"cm\":0,\"sss\":2,\"ses\":3}]}";

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
