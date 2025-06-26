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
import com.couchbase.client.core.topology.BucketCapability;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import com.couchbase.client.core.topology.HostAndServicePorts;
import com.couchbase.client.core.topology.TopologyRevision;
import com.couchbase.client.core.util.HostAndPort;

import java.util.List;
import java.util.NoSuchElementException;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.redactSystem;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A wrapper around a {@link ClusterTopologyWithBucket} that exposes just the bits the DCP client cares about.
 */
public class DcpBucketConfig {
  private final ClusterTopologyWithBucket topology;
  private final CouchbaseBucketTopology bucket;
  private final NodeToPartitionMultimap map;
  private final List<HostAndServicePorts> allKvNodes;

  public DcpBucketConfig(final ClusterTopologyWithBucket topology) {
    this.topology = requireNonNull(topology);
    this.bucket = (CouchbaseBucketTopology) topology.bucket();
    this.map = new NodeToPartitionMultimap(bucket);

    allKvNodes = unmodifiableList(
        topology.nodes().stream()
            .filter(node -> node.has(ServiceType.KV))
            .collect(toList())
    );
  }

  public TopologyRevision rev() {
    return topology.revision();
  }

  public int numberOfPartitions() {
    return bucket.partitions().size();
  }

  public List<HostAndServicePorts> nodes() {
    return allKvNodes;
  }

  public List<PartitionInstance> getHostedPartitions(final HostAndPort nodeAddress) throws NoSuchElementException {
    int nodeIndex = getNodeIndex(nodeAddress);
    return map.get(nodeIndex);
  }

  /**
   * Returns an unmodifiable list containing only those nodes that are running the KV service.
   */
  public List<HostAndServicePorts> getKvNodes() {
    return allKvNodes;
  }

  public int getNodeIndex(final HostAndPort nodeAddress) throws NoSuchElementException {
    int nodeIndex = 0;
    for (HostAndServicePorts node : nodes()) {
      if (nodeAddress.equals(getAddress(node))) {
        return nodeIndex;
      }
      nodeIndex++;
    }
    throw new NoSuchElementException("Failed to locate " + redactSystem(nodeAddress) + " in bucket config.");
  }

  public HostAndPort getActiveNodeKvAddress(int partition) {
    HostAndServicePorts node = bucket.partitions().active(partition)
        .orElseThrow(() -> new IllegalStateException("No active node for partition " + partition));
    return getAddress(node);
  }

  public List<PartitionInstance> getAbsentPartitionInstances() {
    return map.getAbsent();
  }

  public HostAndPort getAddress(final HostAndServicePorts node) {
    return new HostAndPort(
        node.host(),
        node.port(ServiceType.KV)
            .orElseThrow(() -> new IllegalArgumentException("Node not running KV service: " + node)));
  }

  public int numberOfReplicas() {
    return bucket.numberOfReplicas();
  }

  public boolean hasCapability(BucketCapability capability) {
    return bucket.hasCapability(capability);
  }
}
