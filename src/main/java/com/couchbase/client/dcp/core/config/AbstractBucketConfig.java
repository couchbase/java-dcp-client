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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectReader;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.dcp.core.utils.JacksonHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.dcp.core.utils.CbCollections.newEnumSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public abstract class AbstractBucketConfig implements BucketConfig {
  private final String name;
  private final String uuid;
  private final Set<BucketCapability> capabilities;

  public AbstractBucketConfig(
      String name,
      String uuid,
      Set<BucketCapability> capabilities
  ) {
    this.name = requireNonNull(name);
    this.uuid = requireNonNull(uuid);
    this.capabilities = unmodifiableSet(newEnumSet(BucketCapability.class, capabilities));
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

  private static final ObjectReader bucketCapabilitiesReader = JacksonHelper.reader()
      .withFeatures(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
      .forType(new TypeReference<Set<BucketCapability>>() {
      });

  static Set<BucketCapability> parseBucketCapabilities(ObjectNode configNode) {
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
      throw new CouchbaseException("Failed to parse bucketCapabilities node: " + capabilitiesNode);
    }
  }
}
