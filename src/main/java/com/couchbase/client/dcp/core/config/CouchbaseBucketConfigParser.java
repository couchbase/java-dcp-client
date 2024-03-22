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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.dcp.core.utils.JacksonHelper;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.dcp.core.config.AbstractBucketConfig.parseBucketCapabilities;
import static com.couchbase.client.dcp.core.utils.CbCollections.transform;


public class CouchbaseBucketConfigParser {

  public static CouchbaseBucketConfig parse(ObjectNode configNode, List<NodeInfo> nodes) {
    ObjectNode vBucketServerMap = (ObjectNode) configNode.get("vBucketServerMap");

    PartitionMap partitionMap = parsePartitionMap(
        nodes,
        vBucketServerMap.get("vBucketMap")
    ).orElse(PartitionMap.ABSENT);

    Optional<PartitionMap> partitionMapForward = parsePartitionMap(
        nodes,
        vBucketServerMap.get("vBucketMapForward")
    );

    Set<BucketCapability> bucketCapabilities = parseBucketCapabilities(configNode);
    boolean ephemeral = parseEphemeral(configNode, bucketCapabilities);

    return new CouchbaseBucketConfig(
        configNode.path("name").asText(),
        configNode.path("uuid").asText(),
        bucketCapabilities,
        ephemeral,
        vBucketServerMap.path("numReplicas").asInt(0),
        partitionMap,
        partitionMapForward.orElse(null)
    );
  }

  private static boolean parseEphemeral(ObjectNode configNode, Set<BucketCapability> bucketCapabilities) {
    // The "bucketType" field was added in 7.1.0 (along with the Magma storage backend).
    // If present, this field tells us whether the bucket is ephemeral.
    // If absent, a bucket without the "couchapi" capability is ephemeral.
    // Can't just always use the bucket capabilities, because some (all?) versions of Magma do not support couchapi.
    String bucketType = configNode.path("bucketType").textValue();
    return bucketType != null
        ? "ephemeral".equals(bucketType)
        : !bucketCapabilities.contains(BucketCapability.COUCHAPI);
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

}
