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
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerator;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonSerializer;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/**
 * Jackson JSON serializer for {@link SessionState} and {@link PartitionState}.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class SessionStateSerializer extends JsonSerializer<SessionState> {

    @Override
    public void serialize(SessionState ss, final JsonGenerator gen, final SerializerProvider serializers)
        throws IOException {
        gen.writeStartObject();

        gen.writeFieldName("v");
        gen.writeNumber(SessionState.CURRENT_VERSION);

        gen.writeFieldName("ps");
        gen.writeStartArray();
        ss.foreachPartition(partitionState -> {
            try {
                gen.writeObject(partitionState);
            } catch (Exception ex) {
                throw new RuntimeException("Could not serialize PartitionState to JSON: " + partitionState, ex);
            }
        });
        gen.writeEndArray();

        gen.writeEndObject();
    }

}
