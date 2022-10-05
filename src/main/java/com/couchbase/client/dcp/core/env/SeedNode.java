/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.core.env;


import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The {@link SeedNode} represents a combination of hostname/ip and port that is used during the SDK bootstrap.
 * <p>
 * Note that this class is used mostly internally but can be used during bootstrap to override the default ports on
 * a per-node basis. Most of the time you want to use the connection string bootstrap instead.
 */
public class SeedNode {

  /**
   * Seed node set pointing to localhost with default ports.
   */
  public static final Set<SeedNode> LOCALHOST = Collections.singleton(SeedNode.create("127.0.0.1"));

  /**
   * The hostname or IP used.
   */
  private final String address;

  /**
   * If present, an alternate KV port.
   */
  private final Optional<Integer> kvPort;

  /**
   * If present, an alternate HTTP ("cluster manager") port.
   */
  private final Optional<Integer> clusterManagerPort;

  /**
   * Creates a seed node from a hostname and the default ports.
   *
   * @param address the hostname or IP of the seed node.
   * @return the created {@link SeedNode}.
   */
  public static SeedNode create(final String address) {
    return create(address, Optional.empty(), Optional.empty());
  }

  /**
   * Creates a seed node from a hostname and custom ports.
   *
   * @param address the hostname or IP of the seed node.
   * @return the created {@link SeedNode}.
   */
  public static SeedNode create(final String address, final Optional<Integer> kvPort,
                                final Optional<Integer> clusterManagerPort) {
    return new SeedNode(address, kvPort, clusterManagerPort);
  }

  private SeedNode(final String address, final Optional<Integer> kvPort, final Optional<Integer> clusterManagerPort) {
    this.address = requireNonNull(address, "Address");
    this.kvPort = requireNonNull(kvPort, "KvPort");
    this.clusterManagerPort = requireNonNull(clusterManagerPort, "ClusterManagerPort");
  }

  /**
   * The ip address or hostname of this seed node.
   */
  public String address() {
    return address;
  }

  /**
   * If present, the kv port.
   */
  public Optional<Integer> kvPort() {
    return kvPort;
  }

  public SeedNode withKvPort(int port) {
    return new SeedNode(address, port == 0 ? Optional.empty() : Optional.of(port), clusterManagerPort);
  }

  public SeedNode withManagerPort(int port) {
    return new SeedNode(address, kvPort, port == 0 ? Optional.empty() : Optional.of(port));
  }

  private static final int DEFAULT_KV_PORT = 11210;
  private static final int DEFAULT_MANAGER_PORT = 8091;
  private static final int DEFAULT_KV_TLS_PORT = 11207;
  private static final int DEFAULT_MANAGER_TLS_PORT = 18091;

  public SeedNode withDefaultPortsIfNoneSpecified(boolean tls) {
    if (clusterManagerPort.isPresent() || kvPort.isPresent()) {
      return this;
    }

    return withManagerPort(tls ? DEFAULT_MANAGER_TLS_PORT : DEFAULT_MANAGER_PORT)
        .withKvPort(tls ? DEFAULT_KV_TLS_PORT : DEFAULT_KV_PORT);
  }

  /**
   * If present, the cluster manager port.
   */
  public Optional<Integer> clusterManagerPort() {
    return clusterManagerPort;
  }

  @Override
  public String toString() {
    return "SeedNode{" +
        "address='" + address + '\'' +
        ", kvPort=" + kvPort +
        ", mgmtPort=" + clusterManagerPort +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SeedNode seedNode = (SeedNode) o;
    return Objects.equals(address, seedNode.address) &&
        Objects.equals(kvPort, seedNode.kvPort) &&
        Objects.equals(clusterManagerPort, seedNode.clusterManagerPort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, kvPort, clusterManagerPort);
  }

}
