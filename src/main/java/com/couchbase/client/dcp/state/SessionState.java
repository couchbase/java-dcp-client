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

import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.state.json.SessionStateDeserializer;
import com.couchbase.client.dcp.state.json.SessionStateSerializer;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.couchbase.client.dcp.util.MathUtils.lessThanUnsigned;

/**
 * Holds the state information for the current session (all partitions involved).
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
@JsonSerialize(using = SessionStateSerializer.class)
@JsonDeserialize(using = SessionStateDeserializer.class)
public class SessionState {

  /**
   * Private jackson mapper instance used for encoding and decoding into JSON for the session
   * and partition states.
   */
  private static final ObjectMapper JACKSON = new ObjectMapper();

  /**
   * The highest possible sequence number, {@code (2^64)-1}. Sequence numbers must be interpreted as unsigned values.
   * When comparing two sequence numbers to see which is greater, use {@code Long.compareUnsigned}.
   */
  public static final long NO_END_SEQNO = 0xffffffffffffffffL;

  /**
   * The current version format used on export, respected on import to aid backwards compatibility.
   */
  public static final int CURRENT_VERSION = 1;

  /**
   * The maximum number of partitions that can be stored.
   */
  private static final int MAX_PARTITIONS = 1024;

  /**
   * Contains states for each individual partition.
   */
  private final AtomicReferenceArray<PartitionState> partitionStates;

  /**
   * Initializes with an empty partition state for 1024 partitions.
   */
  public SessionState() {
    this.partitionStates = new AtomicReferenceArray<>(MAX_PARTITIONS);
  }

  /**
   * Initializes all partition states to start at the beginning (0) with no end.
   *
   * @param numPartitions the actual number of partitions used.
   */
  public void setToBeginningWithNoEnd(final int numPartitions) {
    if (numPartitions > MAX_PARTITIONS) {
      throw new IllegalArgumentException("Can only hold " + MAX_PARTITIONS + " partitions, " + numPartitions
          + "supplied as initializer.");
    }

    for (int i = 0; i < numPartitions; i++) {
      PartitionState partitionState = new PartitionState();
      partitionState.setEndSeqno(NO_END_SEQNO);
      partitionState.setStartSeqno(0);
      partitionState.setSnapshot(SnapshotMarker.NONE);
      partitionStates.set(i, partitionState);
    }
  }

  /**
   * Recovers the session state from persisted JSON.
   *
   * @param persisted the persisted JSON format.
   */
  public void setFromJson(final byte[] persisted) {
    try {
      SessionState decoded = JACKSON.readValue(persisted, SessionState.class);
      decoded.foreachPartition(new Consumer<PartitionState>() {
        int i = 0;

        @Override
        public void accept(PartitionState dps) {
          partitionStates.set(i++, dps);
        }
      });
    } catch (Exception ex) {
      throw new RuntimeException("Could not decode SessionState from JSON.", ex);
    }
  }

  /**
   * Accessor into the partition state, only use this if really needed.
   * <p>
   * If you want to avoid going out of bounds, use the simpler iterator way on {@link #foreachPartition(Consumer)}.
   *
   * @param partition the index of the partition.
   * @return the partition state for the given partition id.
   */
  public PartitionState get(final int partition) {
    return partitionStates.get(partition);
  }

  /**
   * Accessor to set/override the current partition state, only use this if really needed.
   *
   * @param partition the index of the partition.
   * @param partitionState the partition state to override.
   */
  public void set(int partition, PartitionState partitionState) {
    partitionStates.set(partition, partitionState);
  }

  /**
   * Check if the current sequence numbers for all partitions are >= the ones set as end.
   *
   * @return true if all are at the end, false otherwise.
   */
  public boolean isAtEnd() {
    final AtomicBoolean atEnd = new AtomicBoolean(true);
    foreachPartition(ps -> {
      if (!ps.isAtEnd()) {
        atEnd.set(false);
      }
    });
    return atEnd.get();
  }

  /**
   * Helper method to rollback the given partition to the given sequence number.
   * <p>
   * This will set the seqno AND REMOVE ALL ENTRIES from the failover log that are higher
   * than the given sequence number!
   *
   * @param partition the partition to rollback
   * @param seqno the sequence number where to roll it back to.
   */
  public void rollbackToPosition(int partition, long seqno) {
    PartitionState ps = partitionStates.get(partition);
    ps.setStartSeqno(seqno);
    ps.setSnapshot(new SnapshotMarker(seqno, seqno));
    List<FailoverLogEntry> failoverLog = ps.getFailoverLog();
    Iterator<FailoverLogEntry> flogIterator = failoverLog.iterator();
    List<FailoverLogEntry> entriesToRemove = new ArrayList<>();
    while (flogIterator.hasNext()) {
      FailoverLogEntry entry = flogIterator.next();
      // check if this entry is has a higher seqno than we need to roll back to
      if (lessThanUnsigned(seqno, entry.getSeqno())) {
        entriesToRemove.add(entry);
      }
    }
    failoverLog.removeAll(entriesToRemove);
  }

  /**
   * Provides an iterator over all partitions, calling the callback for each one.
   *
   * @param action the action to be called with the state for every partition.
   */
  public void foreachPartition(final Consumer<PartitionState> action) {
    int len = partitionStates.length();
    for (int i = 0; i < len; i++) {
      PartitionState ps = partitionStates.get(i);
      if (ps == null) {
        continue;
      }
      action.accept(ps);
    }
  }

  public void foreachPartition(final BiConsumer<Integer, PartitionState> consumer) {
    int len = partitionStates.length();
    for (int i = 0; i < len; i++) {
      PartitionState ps = partitionStates.get(i);
      if (ps == null) {
        continue;
      }
      consumer.accept(i, ps);
    }
  }

  /**
   * Export the {@link PartitionState} into the desired format.
   *
   * @param format the format in which the state should be exposed, always uses the current version.
   * @return the exported format, depending on the type can be converted into a string by the user.
   */
  public byte[] export(final StateFormat format) {
    try {
      if (format == StateFormat.JSON) {
        return JACKSON.writeValueAsBytes(this);
      } else {
        throw new IllegalStateException("Unsupported Format " + format);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Could not encode SessionState to Format " + format, ex);
    }
  }

  @Override
  public String toString() {
    return "SessionState[" + partitionStates + ']';
  }
}
