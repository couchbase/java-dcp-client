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

import com.couchbase.client.core.annotation.SinceCouchbase;

import java.util.EnumSet;
import java.util.Set;

/**
 * Flags for "open stream" and "add stream" requests.
 */
public enum StreamFlag {
  /**
   * Specifies that the stream should send over all remaining data to the remote node and
   * then set the remote nodes VBucket to active state and the source nodes VBucket to dead.
   */
  TAKEOVER(0x01),
  /**
   * Specifies that the stream should only send items only if they are on disk. The first
   * item sent is specified by the start sequence number and items will be sent up to the
   * sequence number specified by the end sequence number or the last on disk item when
   * the stream is created.
   */
  DISK_ONLY(0x02),
  /**
   * Specifies that the server should stream all mutations up to the current sequence number
   * for that VBucket. The server will overwrite the value of the end sequence number field
   * with the value of the latest sequence number.
   */
  LATEST(0x04),
  /**
   * Specifies that the server should stream only item key and metadata in the mutations
   * and not stream the value of the item.
   *
   * @deprecated (Removed in 5.0, use OpenConnectionFlag.NO_VALUE when opening the connection instead)
   */
  @Deprecated
  NO_VALUE(0x08),
  /**
   * Indicate the server to add stream only if the VBucket is active.
   * If the VBucket is not active, the stream request fails with ERR_NOT_MY_VBUCKET (0x07)
   */
  ACTIVE_VB_ONLY(0x10),
  /**
   * Indicate the server to check for vb_uuid match even at start_seqno 0 before
   * adding the stream successfully.
   * If the flag is set and there is a vb_uuid mismatch at start_seqno 0, then
   * the server returns ENGINE_ROLLBACK error.
   */
  STRICT_VB_UUID(0x20),

  /**
   * Specifies that the server should skip rollback if the client is behind
   * the purge seqno, but the request is otherwise satisfiable (i.e. no other
   * rollback checks such as UUID mismatch fail). The client could end up
   * missing purged tombstones (and hence could end up never being told about
   * a document deletion). The intent of this flag is to allow clients
   * who ignore deletes to avoid rollbacks to zero which are solely due to them
   * being behind the purge seqno. This flag was added in Couchbase Server 7.2.
   */
  @SinceCouchbase("7.2")
  IGNORE_PURGED_TOMBSTONES(0x80),
  ;

  private final int value;

  StreamFlag(int value) {
    this.value = value;
  }

  public int value() {
    return value;
  }

  public boolean isSet(int flags) {
    return (flags & value) == value;
  }

  public static int encode(Set<StreamFlag> flags) {
    int result = 0;
    for (StreamFlag f : flags) {
      result |= f.value();
    }
    return result;
  }

  public static Set<StreamFlag> decode(int flags) {
    final Set<StreamFlag> result = EnumSet.noneOf(StreamFlag.class);
    for (StreamFlag f : StreamFlag.values()) {
      if (f.isSet(flags)) {
        result.add(f);
      }
    }
    return result;
  }
}
