package com.couchbase.client.dcp.state;


import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class PartitionState {

    private final SortedMap<Long, Long> failoverLog;

    private volatile long startSeqno = 0;
    private volatile long endSeqno = 0;
    private volatile long snapshotStartSeqno = 0;
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

    public long getLastUuid() {
        return failoverLog.lastKey();
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
