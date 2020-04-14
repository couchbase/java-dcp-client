/*
 * Copyright 2018 Couchbase, Inc.
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

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.ResourceLeakDetector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;

@SpringBootApplication
public class Application {
  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private final String username;
  private final String password;
  private final String bootstrapHostnames;

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  public Application(@Value("${username}") String username,
                     @Value("${password}") String password,
                     @Value("${nodes}") String bootstrapHostnames) {
    this.username = username;
    this.password = password;
    this.bootstrapHostnames = bootstrapHostnames;
  }

  @Bean(destroyMethod = "disconnect")
  public Cluster cluster() {
    Cluster cluster = Cluster.connect(bootstrapHostnames, username, password);
    // open a bucket so management API calls succeed against server versions prior to 6.5
    cluster.bucket("default");
    return cluster;
  }

  @Bean
  public BucketManager clusterManager() {
    return cluster().buckets();
  }

  @Bean
  public ObjectMapper jsonRpcObjectMapper() {
    return newLenientObjectMapper();
  }
}
