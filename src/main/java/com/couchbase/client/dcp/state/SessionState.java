package com.couchbase.client.dcp.state;


import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Holds the state information for the current session (all partitions involved).
 */
public class SessionState {

    public static final int NO_END_SEQNO = 0xffffffff;
    public static final int NUM_PARTITIONS = 1024;

    private final AtomicReferenceArray<PartitionState> partitionStates;

    /**
     * Initializes with an empty partition state for 1024 partitions.
     */
    public SessionState() {
        this.partitionStates = new AtomicReferenceArray<PartitionState>(NUM_PARTITIONS);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitionStates.set(i, new PartitionState());
        }
    }

    public void intializeToBeginningWithNoEnd() {
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            PartitionState partitionState = partitionStates.get(i);
            partitionState.setEndSeqno(NO_END_SEQNO);
            partitionStates.set(i, partitionState);
        }
    }

    public PartitionState get(int partiton) {
        return partitionStates.get(partiton);
    }

    public void set(int partition, PartitionState partitionState) {
        partitionStates.set(partition, partitionState);
    }

}
