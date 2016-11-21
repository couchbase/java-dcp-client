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

package com.couchbase.client.dcp.events;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;
import com.couchbase.client.dcp.message.StreamEndReason;

import java.util.Map;

/**
 * Event published when stream has stopped activity.
 */
public class StreamEndEvent implements CouchbaseEvent {
    private final short partition;
    private final StreamEndReason reason;

    public StreamEndEvent(short partition, StreamEndReason reason) {
        this.partition = partition;
        this.reason = reason;
    }

    @Override
    public EventType type() {
        return EventType.SYSTEM;
    }

    public short partition() {
        return partition;
    }

    public StreamEndReason reason() {
        return reason;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = Events.identityMap(this);
        result.put("partition", partition);
        result.put("reason", reason);
        return result;
    }

    @Override
    public String toString() {
        return "StreamEndEvent{" +
                "partition=" + partition +
                "reason=" + reason +
                '}';
    }
}
