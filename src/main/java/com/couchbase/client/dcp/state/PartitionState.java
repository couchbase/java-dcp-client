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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnore;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.KeyExtractor;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.couchbase.client.dcp.util.MathUtils.lessThanUnsigned;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * Represents the individual current session state for a given partition.
 */
public class PartitionState {

  /**
   * Stores the failover log for this partition.
   */
  @JsonProperty("flog")
  private volatile List<FailoverLogEntry> failoverLog = new CopyOnWriteArrayList<>();

  /**
   * Stores the "starting" sequence number for this partition.
   * <p>
   * Updated whenever the client receives a data event,
   * and used to automatically resume streaming if necessary.
   */
  @JsonProperty("ss")
  private volatile long startSeqno = 0;

  /**
   * Stores the ending sequence number for this partition.
   */
  @JsonProperty("es")
  private volatile long endSeqno = 0;

  /**
   * Snapshot boundaries associated with the current {@link #startSeqno}.
   */
  private volatile SnapshotMarker snapshot = SnapshotMarker.NONE;

  /**
   * A snapshot marker received from the server, for which no data event
   * has yet been received.
   * <p>
   * Will be associated with <i>future</i> data events.
   * <p>
   * Tracked separately, because the server might drop the connection
   * after sending the snapshot marker and before sending the first
   * data event in the snapshot.
   * <p>
   * Null if a data event has been received after the most recent
   * snapshot marker event.
   */
  @Nullable
  private volatile SnapshotMarker pendingSnapshot = null;

  /**
   * Stores the collections manifest UID for this partition so that the client
   * can receive consistent manifest updates when the stream is restarted.
   */
  @JsonProperty("cm")
  private volatile long collectionsManifestUid;

  /**
   * This partition's view of the collection manifest. Might be more recent
   * than the {@link #collectionsManifestUid} property (which tracks the stream
   * seqnos and is part of the stream offset).
   * <p>
   * Used while the stream is open to look up scope and collection names.
   * Unrelated to the stream offset.
   */
  private volatile CollectionsManifest collectionsManifest;

  /**
   * This is logically scoped to the channel, but it's convenient
   * to have one per partition.
   */
  private volatile KeyExtractor keyExtractor;

  /**
   * For diagnostic purposes, records the offset used for this partition's
   * most recent "open stream" request.
   */
  @JsonIgnore
  private volatile StreamOffset mostRecentOpenStreamOffset;

  public static PartitionState fromOffset(StreamOffset offset) {
    PartitionState ps = new PartitionState();
    ps.setStartSeqno(offset.getSeqno(), offset.getSnapshot());
    ps.setEndSeqno(-1L);
    ps.setCollectionsManifestUid(offset.getCollectionsManifestUid());

    // Use seqno -1 (max unsigned) so this synthetic failover log entry will always be pruned
    // if the initial streamOpen request gets a rollback response. If there's no rollback
    // on initial request, then the seqno used here doesn't matter, because the failover log
    // gets reset when the stream is opened.
    ps.setFailoverLog(singletonList(new FailoverLogEntry(-1L, offset.getVbuuid())));
    return ps;
  }

  public long getCollectionsManifestUid() {
    return collectionsManifestUid;
  }

  public void setCollectionsManifestUid(long collectionsManifestUid) {
    this.collectionsManifestUid = collectionsManifestUid;
  }

  @JsonIgnore
  public CollectionsManifest getCollectionsManifest() {
    if (collectionsManifest == null) {
      throw new IllegalStateException("Collection manifest not yet set.");
    }
    return collectionsManifest;
  }

  public void setCollectionsManifest(CollectionsManifest collectionsManifest) {
    this.collectionsManifest = requireNonNull(collectionsManifest);
  }

  @JsonIgnore
  public KeyExtractor getKeyExtractor() {
    if (keyExtractor == null) {
      throw new IllegalStateException("Key extractor not yet set.");
    }
    return keyExtractor;
  }

  public void setKeyExtractor(KeyExtractor keyExtractor) {
    this.keyExtractor = keyExtractor;
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
   * Use this when initializing or resetting the partition's sequence number.
   */
  public void setStartSeqno(long startSeqno, SnapshotMarker snapshot) {
    this.startSeqno = startSeqno;
    this.snapshot = requireNonNull(snapshot);
    this.pendingSnapshot = null;
  }

  /**
   * Use this when advancing the partition's sequence number in response
   * to a data event.
   * <p>
   * If there is a pending snapshot marker, it is promoted to "current"
   * and associated with the given seqno (and with all future seqnos
   * until the server sends another snapshot marker).
   */
  public void setStartSeqno(long startSeqno) {
    this.startSeqno = startSeqno;

    SnapshotMarker nonVolatilePending = pendingSnapshot;
    if (nonVolatilePending != null) {
      snapshot = nonVolatilePending;
      pendingSnapshot = null;
    }
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
   * Sets the failover log.
   */
  public void setFailoverLog(List<FailoverLogEntry> log) {
    failoverLog = new CopyOnWriteArrayList<>(log);
  }

  /**
   * Add a new seqno/uuid combination to the failover log.
   *
   * @param seqno the sequence number.
   * @param vbuuid the uuid for the sequence.
   * @deprecated in favor of {@link #setFailoverLog(List)}
   */
  @Deprecated
  public void addToFailoverLog(long seqno, long vbuuid) {
    failoverLog.add(new FailoverLogEntry(seqno, vbuuid));
  }

  /**
   * Returns the current snapshot start sequence number.
   */
  @JsonProperty("sss")
  public long getSnapshotStartSeqno() {
    return snapshot.getStartSeqno();
  }

  /**
   * Allows to set the current snapshot start sequence number.
   *
   * @deprecated in favor of {@link #setSnapshot(SnapshotMarker)}
   */
  @Deprecated
  public void setSnapshotStartSeqno(long snapshotStartSeqno) {
    setSnapshot(new SnapshotMarker(snapshotStartSeqno, this.snapshot.getEndSeqno()));
  }

  @JsonIgnore
  private void setSnapshot(SnapshotMarker snapshot) {
    this.snapshot = requireNonNull(snapshot);
  }

  @JsonIgnore
  public void setPendingSnapshot(SnapshotMarker snapshot) {
    this.pendingSnapshot = requireNonNull(snapshot);
  }

  @JsonIgnore
  public SnapshotMarker getSnapshot() {
    return this.snapshot;
  }

  /**
   * Returns the current snapshot end sequence number.
   */
  @JsonProperty("ses")
  public long getSnapshotEndSeqno() {
    return snapshot.getEndSeqno();
  }

  /**
   * Allows to set the current snapshot end sequence number.
   *
   * @deprecated in favor of {@link #setSnapshot(SnapshotMarker)}
   */
  @Deprecated
  public void setSnapshotEndSeqno(long snapshotEndSeqno) {
    setSnapshot(new SnapshotMarker(this.snapshot.getStartSeqno(), snapshotEndSeqno));
  }

  /**
   * Check if the current partition is at the end (start &gt;= end seqno).
   */
  @JsonIgnore
  public boolean isAtEnd() {
    // Because sequence numbers must be interpreted as unsigned, we can't use the built-in >= operator.
    // For example, when streaming "to infinity" the end seqno consists of 64 "1" bits, which is either
    // -1 or (2^64)-1 depending on whether it's interpreted as signed or unsigned.
    return !lessThanUnsigned(startSeqno, endSeqno);
  }

  /**
   * Convenience method to get the last UUID returned on the failover log.
   * <p>
   * Note that if the failover log is empty, 0 is sent out to indicate the start.
   * <p>
   * The server inserts failover records into the head of the list,
   * so the first one is the most recent.
   */
  @JsonIgnore
  public long getLastUuid() {
    return failoverLog.isEmpty() ? 0 : failoverLog.get(0).getUuid();
  }

  @JsonIgnore
  public StreamOffset getOffset() {
    return new StreamOffset(getLastUuid(), getStartSeqno(), getSnapshot(), getCollectionsManifestUid());
  }

  @Nullable
  public StreamOffset getMostRecentOpenStreamOffset() {
    return mostRecentOpenStreamOffset;
  }

  public void setMostRecentOpenStreamOffset(@Nullable StreamOffset offset) {
    this.mostRecentOpenStreamOffset = offset;
  }

  @Override
  public String toString() {
    return "{" +
        "log=" + failoverLog +
        ", ss=" + startSeqno +
        ", es=" + endSeqno +
        ", cm=" + collectionsManifestUid +
        ", sss=" + getSnapshotStartSeqno() +
        ", ses=" + getSnapshotEndSeqno() +
        '}';
  }
}
