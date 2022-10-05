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


import com.couchbase.client.dcp.core.env.NetworkResolution;

import java.util.List;
import java.util.Set;

import static com.couchbase.client.dcp.core.utils.CbCollections.copyToUnmodifiableList;
import static com.couchbase.client.dcp.core.utils.CbCollections.newEnumSet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;


public class ClusterConfig {
  private final ConfigRevision revision;
  private final List<NodeInfo> nodes;
  private final Set<ClusterCapability> capabilities;
  private final NetworkResolution network;

  public ClusterConfig(
      ConfigRevision revision,
      List<NodeInfo> nodes,
      Set<ClusterCapability> capabilities,
      NetworkResolution network
  ) {
    if (network.equals(NetworkResolution.AUTO)) {
      throw new IllegalArgumentException("Must resolve 'auto' network before creating config.");
    }

    this.revision = requireNonNull(revision);
    this.nodes = copyToUnmodifiableList(requireNonNull(nodes));
    this.capabilities = unmodifiableSet(newEnumSet(ClusterCapability.class, capabilities));
    this.network = requireNonNull(network);
  }

  public ConfigRevision revision() {
    return revision;
  }

  public List<NodeInfo> nodes() {
    return nodes;
  }

  public NetworkResolution network() {
    return network;
  }

  public boolean hasCapability(ClusterCapability capability) {
    return capabilities.contains(capability);
  }

  public Set<ClusterCapability> capabilities() {
    return capabilities;
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
        "revision=" + revision +
        ", nodes=" + nodes +
        ", capabilities=" + capabilities +
        ", network=" + network +
        '}';
  }
}
