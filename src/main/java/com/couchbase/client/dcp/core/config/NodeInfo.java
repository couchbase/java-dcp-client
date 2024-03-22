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

import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.service.ServiceType;

import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.dcp.core.utils.CbCollections.newEnumMap;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Used for locating the services running on a node.
 * <p>
 * Consists of a host (hostname or IP literal) and a map from service to port number.
 * <p>
 * The ports are either all TLS ports, or all non-TLS ports, depending on
 * the {@link PortSelector} used by the config parser.
 */
public class NodeInfo {
  // Placeholder for a node that can't be reached, because it doesn't have an alternate address
  // for the requested network. (Can't just ignore it, because bucket config refers to nodes by index.)
  public static final NodeInfo INACCESSIBLE = new NodeInfo("<inaccessible>", emptyMap());

  private final String host;
  private final Map<ServiceType, Integer> ports;

  public NodeInfo(String host, Map<ServiceType, Integer> ports) {
    this.host = requireNonNull(host);
    this.ports = unmodifiableMap(newEnumMap(ServiceType.class, ports));
  }

  public boolean inaccessible() {
    return this == INACCESSIBLE;
  }

  public NodeIdentifier id() {
    return new NodeIdentifier(host, port(ServiceType.MANAGER).orElse(0));
  }

  public String host() {
    return host;
  }

  public OptionalInt port(ServiceType serviceType) {
    Integer port = ports.get(serviceType);
    return port == null ? OptionalInt.empty() : OptionalInt.of(port);
  }

  public Map<ServiceType, Integer> ports() {
    return ports;
  }

  public boolean has(ServiceType serviceType) {
    return ports.containsKey(serviceType);
  }

  public boolean matches(SeedNode seedNode) {
    return this.host.equals(seedNode.address()) &&
        (portEquals(ServiceType.KV, seedNode.kvPort().orElse(0)) ||
            portEquals(ServiceType.MANAGER, seedNode.clusterManagerPort().orElse(0)));
  }

  private boolean portEquals(ServiceType serviceType, int port) {
    int actualPort = port(serviceType).orElse(0);
    return actualPort != 0 && actualPort == port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeInfo that = (NodeInfo) o;
    return host.equals(that.host) && ports.equals(that.ports);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, ports);
  }

  @Override
  public String toString() {
    return "NodeConfig{" +
        "host='" + redactSystem(host) + '\'' +
        ", serviceToPort=" + redactSystem(ports) +
        '}';
  }

  /**
   * @deprecated Please use {@link #host()} instead.
   */
  @Deprecated
  public String hostname() {
    return host();
  }

  /**
   * @deprecated Please use {@link #ports()} instead.
   */
  @Deprecated
  public Map<ServiceType, Integer> services() {
    return ports();
  }

  /**
   * @deprecated Please use {@link #ports()} instead.
   */
  @Deprecated
  public Map<ServiceType, Integer> sslServices() {
    return ports();
  }
}
