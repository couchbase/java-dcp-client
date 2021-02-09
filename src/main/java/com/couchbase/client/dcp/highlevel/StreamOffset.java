/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp.highlevel;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Position in a DCP stream for a particular vbucket.
 */
public class StreamOffset {
  private final long vbuuid; // vbucket uuid for which the sequence number is valid
  private final long seqno; // sequence number of a DCP event
  private final SnapshotMarker snapshot;
  private final long collectionsManifestUid;

  public static final StreamOffset ZERO = new StreamOffset(0, 0, SnapshotMarker.NONE, 0);

  /**
   * @param collectionsManifestUid pass zero if client is not collections-aware or if no UID is available.
   */
  public StreamOffset(long vbuuid, long seqno, SnapshotMarker snapshot, long collectionsManifestUid) {
    if (Long.compareUnsigned(seqno, snapshot.getStartSeqno()) < 0
        || Long.compareUnsigned(seqno, snapshot.getEndSeqno()) > 0) {
      throw new IllegalArgumentException("Sequence number " + seqno + " is not within snapshot " + snapshot);
    }

    this.vbuuid = vbuuid;
    this.seqno = seqno;
    this.snapshot = requireNonNull(snapshot);
    this.collectionsManifestUid = collectionsManifestUid;
  }

  public long getVbuuid() {
    return vbuuid;
  }

  /**
   * <b>NOTE:</b> Sequence numbers are unsigned, and must be compared using
   * {@link Long#compareUnsigned(long, long)}
   */
  public long getSeqno() {
    return seqno;
  }

  public SnapshotMarker getSnapshot() {
    return snapshot;
  }

  public long getCollectionsManifestUid() {
    return collectionsManifestUid;
  }

  @Override
  public String toString() {
    return vbuuid + "@" + seqno + "m" + collectionsManifestUid + snapshot;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamOffset that = (StreamOffset) o;
    return vbuuid == that.vbuuid &&
        seqno == that.seqno &&
        collectionsManifestUid == that.collectionsManifestUid &&
        snapshot.equals(that.snapshot);
  }

  @Override
  public int hashCode() {
    return Objects.hash(vbuuid, seqno, collectionsManifestUid);
  }
}
