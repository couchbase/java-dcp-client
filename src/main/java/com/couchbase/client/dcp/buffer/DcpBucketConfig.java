/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.buffer;

import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.dcp.core.config.BucketCapability;
import com.couchbase.client.dcp.core.config.ConfigRevision;
import com.couchbase.client.dcp.core.config.CouchbaseBucketConfig;
import com.couchbase.client.dcp.core.config.NodeInfo;

import java.util.List;
import java.util.NoSuchElementException;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.redactSystem;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A wrapper around a {@link CouchbaseBucketConfig} that automatically resolves alternate addresses
 * and analyzes the partition map to support persistence polling.
 */
public class DcpBucketConfig {
  private final CouchbaseBucketConfig config;
  private final NodeToPartitionMultimap map;
  private final List<NodeInfo> allKvNodes;

  public DcpBucketConfig(final CouchbaseBucketConfig config) {
    this.config = requireNonNull(config);
    this.map = new NodeToPartitionMultimap(config);

    allKvNodes = unmodifiableList(
        config.nodes().stream()
            .filter(node -> node.has(ServiceType.KV))
            .collect(toList())
    );
  }

  public ConfigRevision rev() {
    return config.revision();
  }

  public int numberOfPartitions() {
    return config.partitions().size();
  }

  public List<NodeInfo> nodes() {
    return allKvNodes;
  }

  public List<PartitionInstance> getHostedPartitions(final HostAndPort nodeAddress) throws NoSuchElementException {
    int nodeIndex = getNodeIndex(nodeAddress);
    return map.get(nodeIndex);
  }

  /**
   * Returns an unmodifiable list containing only those nodes that are running the KV service.
   */
  public List<NodeInfo> getKvNodes() {
    return allKvNodes;
  }

  public int getNodeIndex(final HostAndPort nodeAddress) throws NoSuchElementException {
    int nodeIndex = 0;
    for (NodeInfo node : nodes()) {
      if (nodeAddress.equals(getAddress(node))) {
        return nodeIndex;
      }
      nodeIndex++;
    }
    throw new NoSuchElementException("Failed to locate " + redactSystem(nodeAddress) + " in bucket config.");
  }

  public HostAndPort getActiveNodeKvAddress(int partition) {
    NodeInfo node = config.partitions().active(partition)
        .orElseThrow(() -> new IllegalStateException("No active node for partition " + partition));
    return getAddress(node);
  }

  public List<PartitionInstance> getAbsentPartitionInstances() {
    return map.getAbsent();
  }

  public HostAndPort getAddress(final NodeInfo node) {
    return new HostAndPort(
        node.host(),
        node.port(ServiceType.KV)
            .orElseThrow(() -> new IllegalArgumentException("Node not running KV service: " + node)));
  }

  public int numberOfReplicas() {
    return config.numberOfReplicas();
  }

  public boolean hasCapability(BucketCapability capability) {
    return config.hasCapability(capability);
  }
}
