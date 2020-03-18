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
  VIEW(BucketServiceMapping.ONE_FOR_ALL),

  /**
   * Key/Value type operations.
   */
  BINARY(BucketServiceMapping.ONE_BY_ONE),

  /**
   * Query (N1QL) operations.
   */
  QUERY(BucketServiceMapping.ONE_FOR_ALL),

  /**
   * HTTP config operations.
   */
  CONFIG(BucketServiceMapping.ONE_FOR_ALL),

  /**
   * Search (CBFT) operations.
   */
  SEARCH(BucketServiceMapping.ONE_FOR_ALL),

  /**
   * Analytics operations.
   */
  ANALYTICS(BucketServiceMapping.ONE_FOR_ALL);

  // If adding a new ServiceType, add to RingBufferMonitor

  private final BucketServiceMapping mapping;

  private ServiceType(BucketServiceMapping mapping) {
    this.mapping = mapping;
  }

  public BucketServiceMapping mapping() {
    return mapping;
  }
}
