package com.couchbase.client.dcp.state;


import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonGenerator;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonToken;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.*;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import rx.functions.Action1;
import rx.functions.Func1;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Holds the state information for the current session (all partitions involved).
 */
@JsonSerialize(using = SessionState.SessionStateSerializer.class)
@JsonDeserialize(using = SessionState.SessionStateDeserializer.class)
public class SessionState {

    private static final ObjectMapper JACKSON = new ObjectMapper();

    public static final int NO_END_SEQNO = 0xffffffff;

    private final AtomicReferenceArray<PartitionState> partitionStates;

    /**
     * Initializes with an empty partition state for 1024 partitions.
     */
    public SessionState() {
        this.partitionStates = new AtomicReferenceArray<PartitionState>(1024);
    }

    public void setToBeginningWithNoEnd(int numPartitions) {
        for (int i = 0; i < numPartitions; i++) {
            PartitionState partitionState = new PartitionState();
            partitionState.setEndSeqno(NO_END_SEQNO);
            partitionState.setSnapshotEndSeqno(NO_END_SEQNO);
            partitionStates.set(i, partitionState);
        }
    }

    public void setFromJson(byte[] persisted) {
        try {
            SessionState decoded = JACKSON.readValue(persisted, SessionState.class);
            decoded.foreachPartition(new Action1<PartitionState>() {
                int i = 0;
                @Override
                public void call(PartitionState dps) {
                    partitionStates.set(i++, dps);
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException("Could not decode SessionState from JSON.", ex);
        }
    }

    public PartitionState get(int partiton) {
        return partitionStates.get(partiton);
    }

    public void set(int partition, PartitionState partitionState) {
        partitionStates.set(partition, partitionState);
    }

    public boolean isAtEnd() {
        final AtomicBoolean atEnd = new AtomicBoolean(true);
        foreachPartition(new Action1<PartitionState>() {
            @Override
            public void call(PartitionState ps) {
                if (!ps.isAtEnd()) {
                    atEnd.set(false);
                }
            }
        });
        return atEnd.get();
    }

    public void foreachPartition(Action1<PartitionState> action) {
        int len = partitionStates.length();
        for (int i = 0; i < len; i++) {
            PartitionState ps = partitionStates.get(i);
            if (ps == null) {
                break;
            }
            action.call(ps);
        }
    }

    public byte[] toJson() {
        try {
            return JACKSON.writeValueAsBytes(this);
        } catch (Exception ex) {
            throw new RuntimeException("Could not encode SessionState to JSON", ex);
        }
    }


    public static class SessionStateSerializer extends JsonSerializer<SessionState> {

        @Override
        public void serialize(SessionState ss, final JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartArray();
            ss.foreachPartition(new Action1<PartitionState>() {
                @Override
                public void call(PartitionState partitionState) {
                    try {
                        gen.writeObject(partitionState);
                    } catch (Exception ex) {
                        throw new RuntimeException("Could not serialize PartitionState to JSON: " + partitionState, ex);
                    }
                }
            });
            gen.writeEndArray();
        }

    }

    public static class SessionStateDeserializer extends JsonDeserializer<SessionState> {

        @Override
        public SessionState deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonToken current = p.getCurrentToken();
            if (current == JsonToken.START_ARRAY) {
                current = p.nextToken();
            } else {
                throw new IllegalStateException("Did expect array, could not decode session state");
            }

            SessionState ss = new SessionState();
            int i = 0;
            while (current != null && current != JsonToken.END_ARRAY) {
                PartitionState ps = p.readValueAs(PartitionState.class);
                ss.set(i++, ps);
                current = p.nextToken();
            }
            return ss;
        }
    }

}
