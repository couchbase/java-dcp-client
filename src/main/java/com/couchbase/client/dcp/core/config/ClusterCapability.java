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


import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public enum ClusterCapability {
  N1QL_ENHANCED_PREPARED_STATEMENTS("n1ql", "enhancedPreparedStatements"),
  ;

  public static final List<ClusterCapability> VALUES = unmodifiableList(Arrays.asList(values()));

  private final String namespace;
  private final String wireName;

  ClusterCapability(String namespace, String wireName) {
    this.namespace = requireNonNull(namespace);
    this.wireName = requireNonNull(wireName);
  }

  public String namespace() {
    return namespace;
  }

  public String wireName() {
    return wireName;
  }

}
