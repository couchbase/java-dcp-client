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
package com.couchbase.client.dcp.core.service;

/**
 * Represents the different {@link ServiceType}s and how they map onto buckets.
 */
public enum ServiceType {

  /**
   * Views and Design Documents.
   */
  VIEWS(ServiceScope.CLUSTER),

  /**
   * Key/Value type operations.
   */
  KV(ServiceScope.BUCKET),

  /**
   * Query (N1QL) operations.
   */
  QUERY(ServiceScope.CLUSTER),

  /**
   * The Cluster Manager service ("ns server")
   */
  MANAGER(ServiceScope.CLUSTER),

  /**
   * Search (CBFT) operations.
   */
  SEARCH(ServiceScope.CLUSTER),

  /**
   * Analytics operations.
   */
  ANALYTICS(ServiceScope.CLUSTER);

  private final ServiceScope scope;

  private ServiceType(ServiceScope scope) {
    this.scope = scope;
  }

  public ServiceScope scope() {
    return scope;
  }
}
