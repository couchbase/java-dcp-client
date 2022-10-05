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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectReader;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.dcp.core.utils.JacksonHelper;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.dcp.core.utils.CbCollections.transform;
import static java.util.Collections.emptySet;


public class CouchbaseBucketConfigParser {

  /**
   * @param originHost Hostname or IP literal of the address this config was retrieved from.
   * Must not include a port. If it's an IPv6 literal, must not include square brackets.
   * @param portSelector TLS or non-TLS?
   * @param networkSelector A pre-determined network? Or auto-detect based on the seed nodes?
   */
  public static CouchbaseBucketConfig parse(
      byte[] json,
      String originHost,
      PortSelector portSelector,
      NetworkSelector networkSelector
  ) {
    ObjectNode configNode = JacksonHelper.readObject(json);

    ClusterConfig clusterConfig = ClusterConfigParser.parse(
        configNode,
        originHost,
        portSelector,
        networkSelector
    );

    List<NodeInfo> nodes = clusterConfig.nodes();
    ObjectNode vBucketServerMap = (ObjectNode) configNode.get("vBucketServerMap");

    PartitionMap partitionMap = parsePartitionMap(
        nodes,
        vBucketServerMap.get("vBucketMap")
    ).orElse(PartitionMap.ABSENT);

    Optional<PartitionMap> partitionMapForward = parsePartitionMap(
        nodes,
        vBucketServerMap.get("vBucketMapForward")
    );

    return new CouchbaseBucketConfig(
        clusterConfig,
        configNode.path("name").asText(),
        configNode.path("uuid").asText(),
        parseBucketCapabilities(configNode),
        vBucketServerMap.path("numReplicas").asInt(0),
        partitionMap,
        partitionMapForward.orElse(null)
    );
  }

  private static final TypeReference<List<List<Integer>>> LIST_OF_LIST_OF_INTEGER_TYPE =
      new TypeReference<List<List<Integer>>>() {
      };

  private static Optional<PartitionMap> parsePartitionMap(
      List<NodeInfo> allNodes,
      @Nullable JsonNode vBucketMapNode
  ) {
    if (vBucketMapNode == null) {
      return Optional.empty();
    }

    List<List<Integer>> vBucketMap = JacksonHelper.convertValue(vBucketMapNode, LIST_OF_LIST_OF_INTEGER_TYPE);
    List<PartitionInfo> entries = transform(vBucketMap, activeAndReplicaNodeIndexes ->
        PartitionInfo.parse(allNodes, activeAndReplicaNodeIndexes)
    );
    return Optional.of(new PartitionMap(entries));
  }

  private static final ObjectReader bucketCapabilitiesReader = JacksonHelper.reader()
      .withFeatures(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
      .forType(new TypeReference<Set<BucketCapability>>() {
      });

  private static Set<BucketCapability> parseBucketCapabilities(ObjectNode configNode) {
    JsonNode capabilitiesNode = configNode.get("bucketCapabilities");
    if (capabilitiesNode == null) {
      return emptySet();
    }
    try {
      Set<BucketCapability> result = bucketCapabilitiesReader.readValue(capabilitiesNode);
      return result.stream()
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
    } catch (IOException e) {
      throw new ConfigException("Failed to parse bucketCapabilities node: " + capabilitiesNode);
    }
  }

}
