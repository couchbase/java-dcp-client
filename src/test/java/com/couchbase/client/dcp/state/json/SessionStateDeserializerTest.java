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

import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SessionStateDeserializerTest {
  @Test
  void deserialize() throws Exception {
    SessionState sessionState = new SessionState();

    String json = "{\"v\":1,\"ps\":[{\"flog\":[{\"seqno\":5,\"uuid\":12345}],\"ss\":1,\"es\":1000,\"sss\":2,\"ses\":3}]}";
    sessionState.setFromJson(json.getBytes(UTF_8));

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
