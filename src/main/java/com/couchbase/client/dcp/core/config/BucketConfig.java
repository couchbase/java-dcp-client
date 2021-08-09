/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.dcp.core.config;

import com.couchbase.client.dcp.core.service.ServiceType;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * Represents a Couchbase Bucket Configuration.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "nodeLocator")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CouchbaseBucketConfig.class, name = "vbucket")})
public interface BucketConfig {

  /**
   * Returns the UUID of the bucket, or {@code null} if the bucket does not have a UUID.
   * <p>
   * The UUID is an opaque value assigned when the bucket is created.
   * If the bucket is deleted and a new bucket is created with the same name,
   * the new bucket will have a different UUID.
   *
   * @return bucket UUID, or {@code null}.
   */
  String uuid();

  /**
   * The name of the bucket.
   *
   * @return name of the bucket.
   */
  String name();

  /**
   * User authorized for the bucket access.
   *
   * @return username of the user.
   */
  String username();

  /**
   * Set user authorized for the bucket access.
   *
   * @param username the user authorized for bucket access
   * @return the config for chaining
   */
  BucketConfig username(String username);

  /**
   * The password of the bucket/user.
   *
   * @return the password of the bucket/user.
   */
  String password();

  /**
   * Setter to inject the password manually into the config.
   *
   * @param password the password of the bucket/user to inject.
   * @return the config for proper chaining.
   */
  BucketConfig password(String password);

  /**
   * The type of node locator in use for this bucket.
   *
   * @return the node locator type.
   */
  BucketNodeLocator locator();

  /**
   * The HTTP Uri for this bucket configuration.
   *
   * @return the uri.
   */
  String uri();

  /**
   * The HTTP Streaming URI for this bucket.
   *
   * @return the streaming uri.
   */
  String streamingUri();

  /**
   * The list of nodes associated with this bucket.
   *
   * @return the list of nodes.
   */
  List<NodeInfo> nodes();

  /**
   * Returns true if the config indicates the cluster is undergoing
   * a transition (such as a rebalance operation).
   *
   * @return true if a transition is in progress.
   */
  boolean tainted();

  /**
   * Revision number (optional) for that configuration.
   *
   * @return the rev number, might be 0.
   */
  BucketConfigRevision rev();

  /**
   * The bucket type.
   *
   * @return the bucket type.
   */
  BucketType type();

  /**
   * Check if the service is enabled on the bucket.
   *
   * @param type the type to check.
   * @return true if it is, false otherwise.
   */
  boolean serviceEnabled(ServiceType type);

  /**
   * Returns true if the config has a fast forward map that describes what the
   * topology of the cluster will be after the current rebalance operation completes.
   *
   * @return true if it does, false otherwise.
   */
  boolean hasFastForwardMap();

  /**
   * Non null if alternate addresses should be used, false otherwise.
   */
  String useAlternateNetwork();

  /**
   * Setter to set if external networking should be used or not.
   *
   * @param useAlternateNetwork if an alternate network should be used.
   */
  void useAlternateNetwork(String useAlternateNetwork);

  /**
   * Returns the bucket capabilities of this bucket config.
   */
  List<BucketCapabilities> capabilities();

}
