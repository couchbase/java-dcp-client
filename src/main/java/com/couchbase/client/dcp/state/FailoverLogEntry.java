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
package com.couchbase.client.dcp.state;

/**
 * Represents a single entry in a failover log per partition state.
 *
 * @since 1.0.0
 * @author Michael Nitschinger
 */
public class FailoverLogEntry {

    private final long seqno;
    private final long uuid;

    public FailoverLogEntry(long seqno, long uuid) {
        this.seqno = seqno;
        this.uuid = uuid;
    }

    public long getSeqno() {
        return seqno;
    }

    public long getUuid() {
        return uuid;
    }

    @Override
    public String toString() {
        return "FailoverLogEntry{" +
            "seqno=" + seqno +
            ", uuid=" + uuid +
            '}';
    }
}
