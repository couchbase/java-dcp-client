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

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonIgnore;
import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PartitionState {

    @JsonProperty("flog")
    private final SortedMap<Long, Long> failoverLog;

    @JsonProperty("ss")
    private volatile long startSeqno = 0;

    @JsonProperty("es")
    private volatile long endSeqno = 0;

    @JsonProperty("sss")
    private volatile long snapshotStartSeqno = 0;

    @JsonProperty("ses")
    private volatile long snapshotEndSeqno = 0;

    public PartitionState() {
        failoverLog = new ConcurrentSkipListMap<Long, Long>();
    }

    public long getEndSeqno() {
        return endSeqno;
    }

    public long getStartSeqno() {
        return startSeqno;
    }

    public void setStartSeqno(long startSeqno) {
        this.startSeqno = startSeqno;
    }

    public void setEndSeqno(long endSeqno) {
        this.endSeqno = endSeqno;
    }

    public SortedMap<Long, Long> getFailoverLog() {
        return failoverLog;
    }

    public void addToFailoverLog(long seqno, long vbuuid) {
        failoverLog.put(seqno, vbuuid);
    }

    public long getSnapshotStartSeqno() {
        return snapshotStartSeqno;
    }

    public void setSnapshotStartSeqno(long snapshotStartSeqno) {
        this.snapshotStartSeqno = snapshotStartSeqno;
    }

    public long getSnapshotEndSeqno() {
        return snapshotEndSeqno;
    }

    public void setSnapshotEndSeqno(long snapshotEndSeqno) {
        this.snapshotEndSeqno = snapshotEndSeqno;
    }

    @JsonIgnore
    public boolean isAtEnd() {
        return startSeqno == endSeqno;
    }

    @JsonIgnore
    public long getLastUuid() {
        return failoverLog.isEmpty() ? 0 : failoverLog.get(failoverLog.lastKey());
    }

    @Override
    public String toString() {
        return "PartitionState{" +
            "startSeqno=" + startSeqno +
            ", endSeqno=" + endSeqno +
            ", snapshotStartSeqno=" + snapshotStartSeqno +
            ", snapshotEndSeqno=" + snapshotEndSeqno +
            ", failoverLog=" + failoverLog +
            '}';
    }
}
