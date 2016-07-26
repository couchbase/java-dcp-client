package com.couchbase.client.dcp.state;


public class PartitionState {

    private volatile long startSeqno = 0;
    private volatile long endSeqno = 0;
    private volatile long uuid = 0;
    private volatile long snapshotStartSeqno = 0;
    private volatile long snapshotEndSeqno = 0;

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

    public long getUuid() {
        return uuid;
    }

    public void setUuid(long uuid) {
        this.uuid = uuid;
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
            ", uuid=" + uuid +
            ", snapshotStartSeqno=" + snapshotStartSeqno +
            ", snapshotEndSeqno=" + snapshotEndSeqno +
            '}';
    }
}
