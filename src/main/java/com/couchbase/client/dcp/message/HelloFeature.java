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

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public enum HelloFeature {
  // For feature definitions see https://github.com/couchbase/kv_engine/blob/master/docs/BinaryProtocol.md#0x1f-helo

  // ORDER IS SIGNIFICANT! Ordinal value corresponds to feature code.

  UNDEFINED_0,
  DATATYPE,
  TLS,
  TCP_NODELAY,
  MUTATION_SEQNO,
  TCP_DELAY,
  XATTR,
  XERROR,
  SELECT_BUCKET,
  UNDEFINED_9,
  SNAPPY,
  JSON,
  DUPLEX,
  CLUSTERMAP_CHANGE_NOTIFICATION,
  UNORDERED_EXECUTION,
  TRACING,
  ALT_REQUEST,
  SYNC_REPLICATION,
  COLLECTIONS,
  OPEN_TRACING,
  PRESERVE_TTL,
  VATTR;

  // Cache since values() creates a new array each time it is called.
  private static final List<HelloFeature> values = unmodifiableList(Arrays.asList(values()));

  public short code() {
    return (short) ordinal();
  }

  public static HelloFeature forCode(short code) {
    try {
      return values.get(code);
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException("Unrecognized feature code: " + code);
    }
  }

  @Override
  public String toString() {
    return String.format("0x%04x (%s)", code(), name());
  }
}
