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

package com.couchbase.client.dcp.message;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;

/**
 * Flags, used in snapshot messages.
 */
public enum SnapshotMarkerFlag {
  /**
   * Specifies that the snapshot contains in-memory items only.
   */
  MEMORY(0x01),

  /**
   * Specifies that the snapshot contains on-disk items only.
   */
  DISK(0x02),

  /**
   * An internally used flag for intra-cluster replication to help to keep in-memory datastructures look similar.
   */
  CHECKPOINT(0x04),

  /**
   * Specifies that this snapshot marker should return a response once the entire snapshot is received.
   * <p>
   * To acknowledge {@link DcpSnapshotMarkerResponse} have to be sent back with the same opaque value.
   */
  ACK(0x08),

  HISTORY(0x10),

  MAY_CONTAIN_DUPLICATE_KEYS(0x20),
  ;

  private final int value;

  SnapshotMarkerFlag(int value) {
    this.value = value;
  }

  public int value() {
    return value;
  }

  public boolean isSet(int flags) {
    return (flags & value) == value;
  }

  private static final List<SnapshotMarkerFlag> valueList = unmodifiableList(Arrays.asList(values()));

  public static Set<SnapshotMarkerFlag> decode(int flags) {
    final Set<SnapshotMarkerFlag> result = EnumSet.noneOf(SnapshotMarkerFlag.class);
    for (SnapshotMarkerFlag f : valueList) {
      if (f.isSet(flags)) {
        result.add(f);
      }
    }
    return result;
  }
}
