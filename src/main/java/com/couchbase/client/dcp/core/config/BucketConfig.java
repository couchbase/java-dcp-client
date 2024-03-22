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
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Set;

public interface BucketConfig {
  /**
   * Returns the UUID of the bucket.
   * <p>
   * The UUID is an opaque value assigned when the bucket is created.
   * If the bucket is deleted and a new bucket is created with the same name,
   * the new bucket will have a different UUID.
   */
  String uuid();

  /**
   * Returns the name of the bucket.
   */
  String name();

  Set<BucketCapability> capabilities();

  default boolean hasCapability(BucketCapability capability) {
    return capabilities().contains(capability);
  }

  static @Nullable BucketConfig parse(ObjectNode json, List<NodeInfo> nodes) {
    switch (json.path("nodeLocator").asText()) {
      case "vbucket":
        return CouchbaseBucketConfigParser.parse(json, nodes);
      case "ketama":
        return MemcachedBucketConfigParser.parse(json, nodes);
      default:
        // this was a "global" config with no bucket information
        return null;
    }
  }
}
