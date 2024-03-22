/*
 * Copyright 2024 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.service.ServiceType;

import java.util.List;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.filter;
import static com.couchbase.client.dcp.core.config.AbstractBucketConfig.parseBucketCapabilities;

public class MemcachedBucketConfigParser {
  public static MemcachedBucketConfig parse(
      ObjectNode configNode,
      List<NodeInfo> nodes,
      MemcachedHashingStrategy hashingStrategy
  ) {
    Set<BucketCapability> bucketCapabilities = parseBucketCapabilities(configNode);

    List<NodeInfo> kvNodes = filter(nodes, (it) -> it.has(ServiceType.KV));
    KetamaRing<NodeInfo> ketamaRing = KetamaRing.create(kvNodes, hashingStrategy);

    return new MemcachedBucketConfig(
        configNode.path("name").asText(),
        configNode.path("uuid").asText(),
        bucketCapabilities,
        ketamaRing
    );
  }
}
