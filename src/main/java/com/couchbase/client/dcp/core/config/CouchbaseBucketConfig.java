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

import reactor.util.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.dcp.core.utils.CbCollections.newEnumSet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class CouchbaseBucketConfig implements BucketConfig {
  static final int PARTITION_NOT_EXISTENT = -2;

  private final String name;
  private final String uuid;
  private final boolean ephemeral;
  private final Set<BucketCapability> capabilities;
  private final int replicas;
  private final PartitionMap partitions;
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private final Optional<PartitionMap> partitionsForward;
  private final Set<String> primaryPartitionHosts;

  public CouchbaseBucketConfig(
      String name,
      String uuid,
      Set<BucketCapability> capabilities,
      boolean ephemeral,
      int replicas,
      PartitionMap partitions,
      @Nullable PartitionMap partitionsForward
  ) {
    this.name = requireNonNull(name);
    this.uuid = requireNonNull(uuid);
    this.replicas = replicas;
    this.partitions = requireNonNull(partitions);
    this.partitionsForward = Optional.ofNullable(partitionsForward);
    this.capabilities = unmodifiableSet(newEnumSet(BucketCapability.class, capabilities));
    this.ephemeral = ephemeral;

    this.primaryPartitionHosts = unmodifiableSet(
        partitions.values().stream()
            .map(it -> it.active().map(NodeInfo::host).orElse(null))
            .filter(Objects::nonNull)
            .collect(toSet())
    );
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String uuid() {
    return uuid;
  }

  @Override
  public Set<BucketCapability> capabilities() {
    return capabilities;
  }

  public boolean ephemeral() {
    return ephemeral;
  }

  public int numberOfPartitions() {
    return partitions.size();
  }

  public int numberOfReplicas() {
    return replicas;
  }

  public PartitionMap partitions() {
    return partitions;
  }

  public Optional<PartitionMap> partitionsForward() {
    return partitionsForward;
  }

  private PartitionMap partitions(boolean forward) {
    return forward
        ? partitionsForward().orElseThrow(() -> new IllegalStateException("Config has no forward partition map."))
        : partitions();
  }

  /**
   * @deprecated This check is not robust in a world where multiple nodes can share the same host.
   * A safer approach would be to consider the KV port as well.
   */
  @Deprecated
  public boolean hasPrimaryPartitionsOnNode(final String hostname) {
    return primaryPartitionHosts.contains(hostname);
  }

  @Deprecated // temporary bridge?
  public int nodeIndexForActive(int partition, boolean forward) {
    return partitions(forward)
        .get(partition)
        .nodeIndexForActive()
        .orElse(PARTITION_NOT_EXISTENT);
  }

  @Deprecated // temporary bridge?
  public int nodeIndexForReplica(int partition, int replica, boolean forward) {
    return partitions(forward)
        .get(partition)
        .nodeIndexForReplica(replica)
        .orElse(PARTITION_NOT_EXISTENT);
  }

  @Override
  public String toString() {
    return "CouchbaseBucketConfig{" +
        "name='" + name + '\'' +
        ", uuid='" + uuid + '\'' +
        ", ephemeral=" + ephemeral +
        ", capabilities=" + capabilities +
        ", replicas=" + replicas +
        ", partitions=" + partitions +
        ", partitionsForward=" + partitionsForward +
        '}';
  }
}
