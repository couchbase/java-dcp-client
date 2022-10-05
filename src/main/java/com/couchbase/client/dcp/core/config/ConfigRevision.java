/*
 * Copyright 2022 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Comparator;
import java.util.Objects;


public class ConfigRevision implements Comparable<ConfigRevision> {
  private static final Comparator<ConfigRevision> comparator =
      Comparator.comparing(ConfigRevision::epoch)
          .thenComparing(ConfigRevision::rev);

  private final long epoch;
  private final long rev;

  public static ConfigRevision parse(ObjectNode json) {
    int epoch = json.path("epoch").intValue(); // zero if not present (server is too old to know about epochs)
    JsonNode revNode = json.path("rev");
    if (!revNode.isInt()) {
      throw new IllegalArgumentException("Missing or non-integer 'rev' field.");
    }
    return new ConfigRevision(epoch, revNode.intValue());
  }

  public ConfigRevision(long epoch, long rev) {
    this.epoch = epoch;
    this.rev = rev;
  }

  public long epoch() {
    return epoch;
  }

  public long rev() {
    return rev;
  }

  @Override
  public int compareTo(ConfigRevision o) {
    return comparator.compare(this, o);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigRevision that = (ConfigRevision) o;
    return epoch == that.epoch && rev == that.rev;
  }

  @Override
  public String toString() {
    return epoch + "." + rev;
  }

  @Override
  public int hashCode() {
    return Objects.hash(epoch, rev);
  }

  public boolean newerThan(ConfigRevision other) {
    return this.compareTo(other) > 0;
  }
}
