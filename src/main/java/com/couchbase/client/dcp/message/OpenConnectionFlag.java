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

package com.couchbase.client.dcp.message;

import java.util.Set;

public enum OpenConnectionFlag {
  /**
   * Requests a producer is created. If this bit is clear a consumer is created.
   */
  PRODUCER(0x01),

  /**
   * @deprecated Notifier connections are deprecated with no replacement.
   */
  @Deprecated
  NOTIFIER(0x02),

  /**
   * Requests that DCP_MUTATION, DCP_DELETION and DCP_EXPIRATION (if enabled) messages should include any XATTRs
   * associated with the Document.
   */
  INCLUDE_XATTRS(0x04),

  /**
   * Requests that DCP_MUTATION, DCP_DELETION and DCP_EXPIRATION (if enabled) messages do not include the Document.
   */
  NO_VALUE(0x08),

  /**
   * Requests that DCP_DELETION messages include metadata regarding when a document was deleted. When a delete is
   * persisted to disk, it is timestamped and purged from the vbucket after some interval. When 'include delete times'
   * is enabled, deletes which are read from disk will have a timestamp value in the delete-time field, in-memory
   * deletes will have a 0 value in the delete-time field. See DCP deletion command. Note when enabled on a consumer,
   * the consumer expects the client to send the delete-time format DCP delete.
   */
  INCLUDE_DELETE_TIMES(0x20);

  private final int value;

  OpenConnectionFlag(int value) {
    this.value = value;
  }

  public int value() {
    return value;
  }

  public boolean isSet(int flags) {
    return (flags & value) == value;
  }

  public static int encode(Set<OpenConnectionFlag> flags) {
    int result = 0;
    for (OpenConnectionFlag f : flags) {
      result |= f.value();
    }
    return result;
  }

}
