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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Represents the individual current session state for a given partition.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class PartitionState {

    /**
     * Stores the failover log for this partition.
     */
    @JsonProperty("flog")
    private final List<FailoverLogEntry> failoverLog;

    /**
     * Stores the starting sequence number for this partition.
     */
    @JsonProperty("ss")
    private volatile long startSeqno = 0;

    /**
     * Stores the ending sequence number for this partition.
     */
    @JsonProperty("es")
    private volatile long endSeqno = 0;

    /**
     * Stores the snapshot start sequence number for this partition.
     */
    @JsonProperty("sss")
    private volatile long snapshotStartSeqno = 0;

    /**
     * Stores the snapshot end sequence number for this partition.
     */
    @JsonProperty("ses")
    private volatile long snapshotEndSeqno = 0;

    /**
     * Initialize a new partition state.
     */
    public PartitionState() {
        failoverLog = new CopyOnWriteArrayList<FailoverLogEntry>();
    }

    /**
     * Returns the current end sequence number.
     */
    public long getEndSeqno() {
        return endSeqno;
    }

    /**
     * Returns the current start sequence number.
     */
    public long getStartSeqno() {
        return startSeqno;
    }

    /**
     * Allows to set the current start sequence number.
     */
    public void setStartSeqno(long startSeqno) {
        this.startSeqno = startSeqno;
    }

    /**
     * Allows to set the current end sequence number.
     */
    public void setEndSeqno(long endSeqno) {
        this.endSeqno = endSeqno;
    }

    /**
     * Returns the full failover log stored, in sorted order.
     */
    public List<FailoverLogEntry> getFailoverLog() {
        return failoverLog;
    }

    /**
     * Add a new seqno/uuid combination to the failover log.
     *
     * @param seqno the sequence number.
     * @param vbuuid the uuid for the sequence.
     */
    public void addToFailoverLog(long seqno, long vbuuid) {
        failoverLog.add(new FailoverLogEntry(seqno, vbuuid));
    }

    /**
     * Returns the current snapshot start sequence number.
     */
    public long getSnapshotStartSeqno() {
        return snapshotStartSeqno;
    }

    /**
     * Allows to set the current snapshot start sequence number.
     */
    public void setSnapshotStartSeqno(long snapshotStartSeqno) {
        this.snapshotStartSeqno = snapshotStartSeqno;
    }

    /**
     * Returns the current snapshot end sequence number.
     */
    public long getSnapshotEndSeqno() {
        return snapshotEndSeqno;
    }

    /**
     * Allows to set the current snapshot end sequence number.
     */
    public void setSnapshotEndSeqno(long snapshotEndSeqno) {
        this.snapshotEndSeqno = snapshotEndSeqno;
    }

    /**
     * Check if the current partition is at the end (start equals end seqno).
     */
    @JsonIgnore
    public boolean isAtEnd() {
        return startSeqno == endSeqno;
    }

    /**
     * Check if the current partition has reached or passed the end (start greater than end seqno).
     */
    @JsonIgnore
    public boolean hasPassedEnd() {
        return startSeqno >= endSeqno;
    }

    /**
     * Convenience method to get the last UUID returned on the failover log.
     *
     * Note that if the failover log is empty, 0 is sent out to indicate the start.
     *
     * Historically the server inserts failover records into the head of the list,
     * so the first one is the most recent.
     */
    @JsonIgnore
    public long getLastUuid() {
        return failoverLog.isEmpty() ? 0 : failoverLog.get(0).getUuid();
    }

    @Override
    public String toString() {
        return "{" +
            "log=" + failoverLog +
            ", ss=" + startSeqno +
            ", es=" + endSeqno +
            ", sss=" + snapshotStartSeqno +
            ", ses=" + snapshotEndSeqno +
            '}';
    }
}
