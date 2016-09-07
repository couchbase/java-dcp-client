package com.couchbase.client.dcp.state;


import rx.functions.Action1;
import rx.functions.Func1;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class SessionState {

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

}
