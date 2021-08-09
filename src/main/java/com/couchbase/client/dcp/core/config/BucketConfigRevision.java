/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.core.config;

import java.util.Comparator;
import java.util.Objects;

public class BucketConfigRevision implements Comparable<BucketConfigRevision> {
  private static final Comparator<BucketConfigRevision> comparator =
      Comparator.comparing(BucketConfigRevision::epoch)
          .thenComparing(BucketConfigRevision::rev);

  private final long epoch;
  private final long rev;

  public BucketConfigRevision(long epoch, long rev) {
    if (rev < 0) {
      throw new IllegalArgumentException("rev must be non-negative");
    }
    this.epoch = Math.max(0, epoch);
    this.rev = rev;
  }

  public boolean newerThan(BucketConfigRevision other) {
    return compareTo(other) > 0;
  }

  public long epoch() {
    return epoch;
  }

  public long rev() {
    return rev;
  }

  @Override
  public int compareTo(BucketConfigRevision o) {
    return comparator.compare(this, o);
  }

  @Override
  public String toString() {
    return epoch + "." + rev;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BucketConfigRevision that = (BucketConfigRevision) o;
    return epoch == that.epoch && rev == that.rev;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, rev);
  }
}
