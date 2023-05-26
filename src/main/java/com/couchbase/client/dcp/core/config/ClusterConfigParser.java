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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.TextNode;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.dcp.core.utils.JacksonHelper;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.dcp.core.utils.CbCollections.transform;
import static java.util.Collections.emptySet;

public class ClusterConfigParser {
  private ClusterConfigParser() {
    throw new AssertionError("not instantiable");
  }

  public static ClusterConfig parse(
      ObjectNode clusterConfig,
      String originHost,
      PortSelector portSelector,
      NetworkSelector networkSelector
  ) {

    ArrayNode nodesExt = (ArrayNode) clusterConfig.get("nodesExt");
    if (nodesExt == null) {
      throw new CouchbaseException("Couchbase Server version is too old for this SDK; missing 'nodesExt' field.");
    }
    List<Map<NetworkResolution, NodeInfo>> nodes = JacksonHelper.transform(nodesExt, node ->
        NodeInfoParser.parse(addHostnameIfMissing(node, originHost), portSelector)
    );

    NetworkResolution resolvedNetwork = networkSelector.selectNetwork(nodes)
        .orElse(NetworkResolution.DEFAULT); // Hope for the best!

    // Discard node info from networks we don't care about.
    //
    // A network SHOULD have an entry for each node, but the server doesn't
    // currently enforce this. If a node doesn't have an alternate address
    // for the selected network, use an "inaccessible" placeholder to preserve
    // the node indexes required by the KV partition map.
    List<NodeInfo> resolvedNodes = transform(nodes, it -> it.getOrDefault(resolvedNetwork, NodeInfo.INACCESSIBLE));

    return new ClusterConfig(
        ConfigRevision.parse(clusterConfig),
        resolvedNodes,
        parseCapabilities(clusterConfig),
        resolvedNetwork
    );
  }

  /**
   * A single-node cluster can omit the hostname, so patch it in!
   * <p>
   * The server omits the hostname so development nodes initialized with a hostname
   * of "localhost" or "127.0.0.1" are accessible from other machines.
   * Otherwise, the node would say, "My address is localhost!" and clients on
   * other machines would look in the wrong place.
   */
  private static ObjectNode addHostnameIfMissing(JsonNode node, String originHost) {
    ObjectNode obj = (ObjectNode) node;
    if (!node.has("hostname") && node.path("thisNode").asBoolean()) {
      obj.set("hostname", new TextNode(originHost));
    }
    return obj;
  }

  private static final TypeReference<Map<String, Set<String>>> SET_MULTIMAP_TYPE = new TypeReference<Map<String, Set<String>>>() {
  };

  /**
   * Parses the cluster capabilities from a cluster config node. For example, when given this:
   * <pre>
   * {
   *   "clusterCapabilities": {
   *     "n1ql": [
   *       "enhancedPreparedStatements"
   *     ]
   *   }
   * }
   * </pre>
   * returns a set containing {@link ClusterCapability#N1QL_ENHANCED_PREPARED_STATEMENTS}.
   * <p>
   * The map from the server is compressed into a set, because looking up capabilities
   * by service is error-prone and not particularly useful.
   */
  private static Set<ClusterCapability> parseCapabilities(ObjectNode clusterConfig) {
    JsonNode capabilitiesNode = clusterConfig.get("clusterCapabilities");
    if (capabilitiesNode == null) {
      return emptySet();
    }

    Map<String, Set<String>> map = JacksonHelper.convertValue(capabilitiesNode, SET_MULTIMAP_TYPE);

    Set<ClusterCapability> result = EnumSet.noneOf(ClusterCapability.class);
    ClusterCapability.VALUES.forEach(it -> {
      if (map.getOrDefault(it.namespace(), emptySet()).contains(it.wireName())) {
        result.add(it);
      }
    });

    return result;
  }
}
