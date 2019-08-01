/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.buffer;

import java.util.Arrays;

import static com.couchbase.client.dcp.util.MathUtils.lessThanUnsigned;

/**
 * For each partition, holds the persisted sequence number and vbucket UUID for the active and replicas.
 * Used for determining the highest sequence number that has been persisted across all nodes.
 * <p>
 * Thread-safe.
 */
public class PersistedSeqnos {

  private static class VbuuidAndSeqno {
    private long vbuuid;
    private long seqno;
    private boolean absent;

    @Override
    public String toString() {
      return seqno + "@" + Long.toHexString(vbuuid);
    }
  }

  private static class PartitionInfo {
    // Each element corresponds to an instance of the partition
    // (one for the active and one for each replica).
    // Order is determined by the bucket configuration.
    private final VbuuidAndSeqno[] vbuuidAndSeqnos;

    PartitionInfo(final int numReplicas) {
      this.vbuuidAndSeqnos = new VbuuidAndSeqno[numReplicas + 1];
      for (int i = 0; i < vbuuidAndSeqnos.length; i++) {
        vbuuidAndSeqnos[i] = new VbuuidAndSeqno();
      }
    }

    public VbuuidAndSeqno get(final int slot) {
      return vbuuidAndSeqnos[slot];
    }

    @Override
    public String toString() {
      return Arrays.toString(vbuuidAndSeqnos) + "=" + persistedSeqno();
    }

    /**
     * Returns the highest seqno that has been persisted to the active and all replicas,
     * or 0 if the value is currently unknown.
     */
    private long persistedSeqno() {
      // skip over any any absent instances at the start of the array
      int i = indexOfFirstPresentPartitionInstance();
      if (i == -1) {
        return 0;
      }

      final long vbuuid = vbuuidAndSeqnos[i].vbuuid;
      long minSeqno = vbuuidAndSeqnos[i].seqno;

      for (i = i + 1; i < vbuuidAndSeqnos.length; i++) {
        final VbuuidAndSeqno vbuuidAndSeqno = vbuuidAndSeqnos[i];

        if (vbuuidAndSeqno.absent) {
          // Ignore absent partition instances, since there's no way
          // for them to have data that would be lost by rollback.
          continue;
        }

        if (vbuuid != vbuuidAndSeqno.vbuuid) {
          // The partition instances are on different history branches.
          // Wait for them to failover to the same branch. In the mean time,
          // halt all operations that depend on seqno persistence.
          return 0;
        }

        //if (vbuuidAndSeqnos[i].seqno != minSeqno) {
        //    System.out.printf("Holding back at least event because persistence not observed on all nodes");
        //}

        if (lessThanUnsigned(vbuuidAndSeqno.seqno, minSeqno)) {
          minSeqno = vbuuidAndSeqnos[i].seqno;
        }
      }
      return minSeqno;
    }


    private int indexOfFirstPresentPartitionInstance() {
      for (int i = 0; i < vbuuidAndSeqnos.length; i++) {
        if (!vbuuidAndSeqnos[i].absent) {
          return i;
        }
      }
      return -1;
    }
  }

  // @GuardedBy("this")
  private PartitionInfo[] partitionInfo;

  private PersistedSeqnos(final int numPartitions, final int numReplicas) {
    reset(numPartitions, numReplicas);
  }

  /**
   * Returns a new empty instance that will not be usable until it is reset.
   */
  public static PersistedSeqnos uninitialized() {
    return new PersistedSeqnos(0, 0);
  }

  /**
   * Updates the dataset with information about the given partition instance
   */
  public synchronized long update(final PartitionInstance partitionInstance, final long vbuuid, final long seqno) {
    return update(partitionInstance.partition(), partitionInstance.slot(), vbuuid, seqno);
  }

  public synchronized long update(final short partition, final int slot, final long vbuuid, final long seqno) {
    VbuuidAndSeqno vbuuidAndSeqno = partitionInfo[partition].get(slot);
    vbuuidAndSeqno.vbuuid = vbuuid;
    vbuuidAndSeqno.seqno = seqno;
    return persistedSeqnoForPartition(partition);
  }

  public synchronized void markAsAbsent(PartitionInstance absentInstance) {
    partitionInfo[absentInstance.partition()].get(absentInstance.slot()).absent = true;
  }

  public synchronized void reset(final DcpBucketConfig bucketConfig) {
    reset(bucketConfig.numberOfPartitions(), bucketConfig.numberOfReplicas());
  }

  public synchronized void reset(final int numPartitions, final int numReplicas) {
    partitionInfo = new PartitionInfo[numPartitions];

    for (int i = 0; i < partitionInfo.length; i++) {
      partitionInfo[i] = new PartitionInfo(numReplicas);
    }
  }

  private synchronized long persistedSeqnoForPartition(final int partition) {
    return partitionInfo[partition].persistedSeqno();
  }

  public synchronized String toString() {
    return Arrays.toString(partitionInfo);
  }
}
