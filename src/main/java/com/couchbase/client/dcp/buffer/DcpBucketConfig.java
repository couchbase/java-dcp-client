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

import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.CouchbaseException;
import com.couchbase.client.dcp.core.config.AlternateAddress;
import com.couchbase.client.dcp.core.config.CouchbaseBucketConfig;
import com.couchbase.client.dcp.core.config.DefaultNodeInfo;
import com.couchbase.client.dcp.core.config.NodeInfo;
import com.couchbase.client.dcp.core.service.ServiceType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A wrapper around a {@link CouchbaseBucketConfig} that automatically resolves alternate addresses
 * and analyzes the partition map to support persistence polling.
 */
public class DcpBucketConfig {
  private final boolean sslEnabled;
  private final CouchbaseBucketConfig config;
  private final NodeToPartitionMultimap map;
  private final List<NodeInfo> allNodes;
  private final List<NodeInfo> allDataNodes;

  public DcpBucketConfig(final CouchbaseBucketConfig config, final boolean sslEnabled) {
    this.config = requireNonNull(config);
    this.sslEnabled = sslEnabled;
    this.map = new NodeToPartitionMultimap(config);
    this.allNodes = resolveAlternateAddresses(config);

    allDataNodes = unmodifiableList(allNodes.stream()
        .filter(this::hasBinaryService)
        .collect(toList()));
  }

  public long rev() {
    return config.rev();
  }

  public int numberOfPartitions() {
    return config.numberOfPartitions();
  }

  public List<NodeInfo> nodes() {
    return allNodes;
  }

  private static List<NodeInfo> resolveAlternateAddresses(CouchbaseBucketConfig config) {
    return config.nodes().stream()
        .map(DcpBucketConfig::resolveAlternateAddress)
        .collect(toList());
  }

  private static NodeInfo resolveAlternateAddress(NodeInfo nodeInfo) {
    final String networkName = nodeInfo.useAlternateNetwork();
    if (networkName == null) {
      return nodeInfo; // don't use alternate
    }

    final AlternateAddress alternate = nodeInfo.alternateAddresses().get(networkName);
    if (alternate == null) {
      throw new CouchbaseException("Node " + system(nodeInfo.hostname()) + " has no alternate hostname for network [" + networkName + "]");
    }

    final Map<ServiceType, Integer> services = new HashMap<>(nodeInfo.services());
    final Map<ServiceType, Integer> sslServices = new HashMap<>(nodeInfo.sslServices());
    services.putAll(alternate.services());
    sslServices.putAll(alternate.sslServices());

    return new DefaultNodeInfo(alternate.hostname(), services, sslServices, emptyMap());
  }

  public List<PartitionInstance> getHostedPartitions(final HostAndPort nodeAddress) throws NoSuchElementException {
    int nodeIndex = getNodeIndex(nodeAddress);
    return map.get(nodeIndex);
  }

  /**
   * Returns an unmodifiable list containing only those nodes that are running the KV service.
   */
  public List<NodeInfo> getDataNodes() {
    return allDataNodes;
  }

  public int getNodeIndex(final HostAndPort nodeAddress) throws NoSuchElementException {
    int nodeIndex = 0;
    for (NodeInfo node : nodes()) {
      if (nodeAddress.equals(getAddress(node))) {
        return nodeIndex;
      }
      nodeIndex++;
    }
    throw new NoSuchElementException("Failed to locate " + system(nodeAddress) + " in bucket config.");
  }

  public HostAndPort getActiveNodeKvAddress(int partition) {
    final int index = config.nodeIndexForMaster(partition, false);
    final NodeInfo node = nodes().get(index);
    final int port = getServicePortMap(node).get(ServiceType.BINARY);
    return new HostAndPort(node.hostname(), port);
  }

  public List<PartitionInstance> getAbsentPartitionInstances() {
    return map.getAbsent();
  }

  public HostAndPort getAddress(final NodeInfo node) {
    int port = getServicePortMap(node).get(ServiceType.BINARY);
    return new HostAndPort(node.hostname(), port);
  }

  private Map<ServiceType, Integer> getServicePortMap(final NodeInfo node) {
    return sslEnabled ? node.sslServices() : node.services();
  }

  private boolean hasBinaryService(final NodeInfo node) {
    return getServicePortMap(node).containsKey(ServiceType.BINARY);
  }

  public int numberOfReplicas() {
    return config.numberOfReplicas();
  }
}
