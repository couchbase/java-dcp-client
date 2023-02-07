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

import com.couchbase.client.dcp.highlevel.internal.DatabaseChangeEvent;
import com.couchbase.client.dcp.message.SnapshotMarkerFlag;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;

public class SnapshotDetails implements DatabaseChangeEvent {
  // Cache the values since Enum.values() returns a new array on every call.
  private static final List<SnapshotMarkerFlag> flagValues = unmodifiableList(
      Arrays.asList(SnapshotMarkerFlag.values()));

  private final int vbucket;
  private final int flags;
  private final SnapshotMarker marker;

  public SnapshotDetails(int vbucket, int flags, SnapshotMarker marker) {
    this.vbucket = vbucket;
    this.flags = flags;
    this.marker = marker;
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onSnapshot(this);
  }

  @Override
  public int getVbucket() {
    return vbucket;
  }

  public Set<SnapshotMarkerFlag> getFlags() {
    // lazy creation, since most users don't care about flags.
    final Set<SnapshotMarkerFlag> result = EnumSet.noneOf(SnapshotMarkerFlag.class);
    for (SnapshotMarkerFlag f : flagValues) {
      if (f.isSet(flags)) {
        result.add(f);
      }
    }
    return result;
  }

  public int getFlagsAsInt() {
    return flags;
  }

  public SnapshotMarker getMarker() {
    return marker;
  }
}
