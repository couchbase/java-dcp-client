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

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class NodeIdentifier {
  private final String host;
  private final int port;

  public NodeIdentifier(String host, int port) {
    this.host = requireNonNull(host);
    this.port = port;
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NodeIdentifier that = (NodeIdentifier) o;
    return port == that.port && host.equals(that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }
}
