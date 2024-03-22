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

import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.dcp.core.CouchbaseException;

/**
 * A strategy compatible with libcouchbase and related SDKs.
 */
public class StandardMemcachedHashingStrategy implements MemcachedHashingStrategy {

  public static final StandardMemcachedHashingStrategy INSTANCE = new StandardMemcachedHashingStrategy();

  private StandardMemcachedHashingStrategy() {
  }

  @Override
  public String hash(final NodeInfo node, final int repetition) {
    HostAndPort authority = node.ketamaAuthority();
    if (authority == null) {
      throw new CouchbaseException(
          "Oh no! Can't talk to this memcached bucket because the server did not advertise non-TLS KV ports for the default network." +
              " Non-TLS KV ports are required for building the ketama ring that determines where documents are located in the cluster," +
              " even when the connection is secured by TLS."
      );
    }

    // Compatibility note: libcouchbase encloses the host in square brackets
    // if it is an IPv6 literal (contains colons). The Java SDK doesn't do that.
    //
    //   return authority.format() + "-" + repetition; // <-- Here's what libcouchbase does!

    return authority.host() + ":" + authority.port() + "-" + repetition;
  }

}
