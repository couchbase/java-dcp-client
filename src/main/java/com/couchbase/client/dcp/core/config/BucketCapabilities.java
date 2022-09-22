/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

/**
 * Describes the bucket capabilities in an abstract fashion as provided by
 * the server.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public enum BucketCapabilities {

  CBHELLO("cbhello"),
  TOUCH("touch"),
  COUCHAPI("couchapi"),
  CCCP("cccp"),
  XDCR_CHECKPOINTING("xdcrCheckpointing"),
  NODES_EXT("nodesExt"),
  DCP("dcp"),
  XATTR("xattr"),
  SNAPPY("snappy"),
  DURABLE_WRITE("durableWrite"),
  COLLECTIONS("collections");

  private final String raw;

  BucketCapabilities(String raw) {
    this.raw = raw;
  }

  @JsonValue
  public String getRaw() {
    return raw;
  }
}
