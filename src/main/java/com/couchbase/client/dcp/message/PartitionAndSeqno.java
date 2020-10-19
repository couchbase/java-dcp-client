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

import java.util.Objects;

/**
 * An immutable pair consisting of a partition and an associated sequence number.
 */
public class PartitionAndSeqno {
  private final int partition;
  private final long seqno;

  public PartitionAndSeqno(int partition, long seqno) {
    this.partition = partition;
    this.seqno = seqno;
  }

  public int partition() {
    return partition;
  }

  public long seqno() {
    return seqno;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionAndSeqno that = (PartitionAndSeqno) o;
    return partition == that.partition &&
        seqno == that.seqno;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partition, seqno);
  }

  @Override
  public String toString() {
    return "PartitionAndSeqno{" +
        "partition=" + partition +
        ", seqno=" + seqno +
        '}';
  }
}
