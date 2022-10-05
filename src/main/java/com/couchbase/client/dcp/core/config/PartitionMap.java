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

import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static com.couchbase.client.dcp.core.utils.CbCollections.copyToUnmodifiableList;
import static java.util.Collections.emptyList;

/**
 * A map from partition index to info about the partition.
 */
public class PartitionMap {
  static final PartitionMap ABSENT = new PartitionMap(emptyList());

  private final List<PartitionInfo> values;

  public PartitionMap(List<PartitionInfo> values) {
    this.values = copyToUnmodifiableList(values);
  }

  public PartitionInfo get(int partition) {
    try {
      return values.get(partition);
    } catch (IndexOutOfBoundsException e) {
      return PartitionInfo.ABSENT;
    }
  }

  public List<PartitionInfo> values() {
    return values;
  }

  /**
   * Returns the number of partitions.
   */
  public int size() {
    return values.size();
  }

  /**
   * Passes each map entry to the given consumer.
   *
   * @param action First argument is partition index, second argument is info about the associated partition.
   */
  public void forEach(BiConsumer<Integer, PartitionInfo> action) {
    int i = 0;
    for (PartitionInfo entry : values) {
      action.accept(i++, entry);
    }
  }

  /**
   * Returns info about the node hosting the active instance of the given partition (vBucket),
   * or empty if the active is not currently available or there is no such partition.
   */
  public Optional<NodeInfo> active(int partition) {
    return get(partition).active();
  }

  /**
   * Returns info about the nodes hosting replica instances of the given partition (vBucket),
   * or empty if no replicas are currently available or there is no such partition.
   */
  public List<NodeInfo> availableReplicas(int partition) {
    return get(partition).availableReplicas();
  }

  @Override
  public String toString() {
    SortedMap<Integer, PartitionInfo> map = new TreeMap<>();
    forEach(map::put);
    return map.toString();
  }
}
