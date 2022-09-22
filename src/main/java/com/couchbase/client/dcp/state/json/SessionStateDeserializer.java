/*
 * Copyright (c) 2016 Couchbase, Inc.
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

import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonToken;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationContext;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

/**
 * Jackson JSON deserializer for {@link SessionState} and {@link PartitionState}.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class SessionStateDeserializer extends JsonDeserializer<SessionState> {

  @Override
  public SessionState deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonToken current = p.getCurrentToken();

    // we ignore the version for now, since there is only one. go directly to the array of states.
    while (current != JsonToken.START_ARRAY) {
      current = p.nextToken();
    }

    current = p.nextToken();
    int i = 0;
    SessionState ss = new SessionState();
    while (current != null && current != JsonToken.END_ARRAY) {
      PartitionState ps = p.readValueAs(PartitionState.class);
      ss.set(i++, ps);
      current = p.nextToken();
    }
    return ss;
  }
}
