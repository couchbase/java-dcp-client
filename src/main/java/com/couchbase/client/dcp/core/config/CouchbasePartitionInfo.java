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
package com.couchbase.client.dcp.core.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents the partition information for a bucket.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CouchbasePartitionInfo {

  private final int numberOfReplicas;
  private final String[] partitionHosts;
  private final List<Partition> partitions;
  private final List<Partition> forwardPartitions;
  private final boolean tainted;

  CouchbasePartitionInfo(
      @JsonProperty("numReplicas") int numberOfReplicas,
      @JsonProperty("serverList") List<String> partitionHosts,
      @JsonProperty("vBucketMap") List<List<Integer>> partitions,
      @JsonProperty("vBucketMapForward") List<List<Integer>> forwardPartitions) {
    this.numberOfReplicas = numberOfReplicas;
    this.partitionHosts = partitionHosts.toArray(new String[partitionHosts.size()]);
    this.partitions = fromPartitionList(partitions);
    if (forwardPartitions != null && !forwardPartitions.isEmpty()) {
      this.forwardPartitions = fromPartitionList(forwardPartitions);
      this.tainted = true;
    } else {
      this.forwardPartitions = null;
      this.tainted = false;
    }
  }

  public boolean hasFastForwardMap() {
    return forwardPartitions != null;
  }

  public int numberOfReplicas() {
    return numberOfReplicas;
  }

  public String[] partitionHosts() {
    return partitionHosts;
  }

  public List<Partition> partitions() {
    return partitions;
  }

  public List<Partition> forwardPartitions() {
    return forwardPartitions;
  }

  public boolean tainted() {
    return tainted;
  }

  private static List<Partition> fromPartitionList(List<List<Integer>> input) {
    List<Partition> partitions = new ArrayList<>();
    if (input == null) {
      return partitions;
    }

    for (List<Integer> partition : input) {
      int primary = partition.remove(0);
      int[] replicas = new int[partition.size()];
      int i = 0;
      for (int replica : partition) {
        replicas[i++] = replica;
      }
      partitions.add(new DefaultPartition(primary, replicas));
    }
    return partitions;
  }

  @Override
  public String toString() {
    return "PartitionInfo{"
        + "numberOfReplicas=" + numberOfReplicas
        + ", partitionHosts=" + Arrays.toString(partitionHosts)
        + ", partitions=" + partitions
        + ", tainted=" + tainted
        + '}';
  }
}
