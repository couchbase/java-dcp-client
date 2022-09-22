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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a single entry in a failover log per partition state.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class FailoverLogEntry {

  private final long seqno;

  private final long uuid;

  @JsonCreator
  public FailoverLogEntry(@JsonProperty("seqno") long seqno, @JsonProperty("uuid") long uuid) {
    this.seqno = seqno;
    this.uuid = uuid;
  }

  public long getSeqno() {
    return seqno;
  }

  public long getUuid() {
    return uuid;
  }

  @Override
  public String toString() {
    return uuid + "@" + seqno;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FailoverLogEntry that = (FailoverLogEntry) o;

    if (seqno != that.seqno) {
      return false;
    }
    return uuid == that.uuid;
  }

  @Override
  public int hashCode() {
    int result = (int) (seqno ^ (seqno >>> 32));
    result = 31 * result + (int) (uuid ^ (uuid >>> 32));
    return result;
  }
}
