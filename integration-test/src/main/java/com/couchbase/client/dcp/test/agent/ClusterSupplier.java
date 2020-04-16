/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.io.Closeable;
import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static java.util.Objects.requireNonNull;

public class ClusterSupplier implements Supplier<Cluster>, Closeable {

  private final String username;
  private final String password;
  private final String bootstrapHostnames;

  private ClusterEnvironment env;
  private Cluster cluster;

  public ClusterSupplier(String username, String password, String bootstrapHostnames) {
    this.username = requireNonNull(username);
    this.password = requireNonNull(password);
    this.bootstrapHostnames = requireNonNull(bootstrapHostnames);
  }

  public void reset() {
    synchronized (this) {
      if (cluster != null) {
        cluster.disconnect();
        env.shutdown();
        cluster = null;
        env = null;
      }
    }
  }

  public void close() {
    reset();
  }

  public Cluster get() {
    synchronized (this) {
      if (cluster == null) {
        env = ClusterEnvironment.builder()
            // Accommodate slow CI docker environment
            .timeoutConfig(TimeoutConfig.kvTimeout(Duration.ofSeconds(30)))
            .build();

        cluster = Cluster.connect(bootstrapHostnames,
            clusterOptions(username, password)
                .environment(env));

        // open a bucket so management API calls succeed against server versions prior to 6.5
        cluster.bucket("default");
      }
      return cluster;
    }
  }

}
