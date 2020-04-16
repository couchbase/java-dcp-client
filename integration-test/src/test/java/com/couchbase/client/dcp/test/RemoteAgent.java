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

package com.couchbase.client.dcp.test;

import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.test.agent.BucketService;
import com.couchbase.client.dcp.test.agent.ClusterService;
import com.couchbase.client.dcp.test.agent.DocumentService;
import com.couchbase.client.dcp.test.agent.StreamerService;
import com.github.therapi.jsonrpc.client.JdkHttpClient;
import com.github.therapi.jsonrpc.client.ServiceFactory;
import com.google.common.base.Strings;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.dcp.test.agent.StreamerService.ALL_VBUCKETS;
import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertTrue;

/**
 * Client for the JSON-RPC API exposed by the test agent.
 */
public class RemoteAgent {
  private final BucketService bucketService;
  private final DocumentService documentService;
  private final StreamerService streamerService;
  private final ClusterService clusterService;

  private static String getDockerHost() {
    try {
      final String env = System.getenv("DOCKER_HOST");
      return Strings.isNullOrEmpty(env) ? "localhost" : new URI(env).getHost();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public RemoteAgent(AgentContainer agentContainer) {
    this("http://" + getDockerHost() + ":" + agentContainer.getHttpPort() + "/jsonrpc");
  }

  public RemoteAgent(String jsonRpcEndpoint) {
    try {
      JdkHttpClient client = new JdkHttpClient(jsonRpcEndpoint);
      client.setReadTimeout(120, TimeUnit.SECONDS);
      ServiceFactory serviceFactory = new ServiceFactory(newLenientObjectMapper(), client);

      this.bucketService = serviceFactory.createService(BucketService.class);
      this.documentService = serviceFactory.createService(DocumentService.class);
      this.streamerService = serviceFactory.createService(StreamerService.class);
      this.clusterService = serviceFactory.createService(ClusterService.class);

    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Tells the agent to close its Java client Cluster and lazily recreate it.
   * <p>
   * This suppresses all kinds of warnings about the client trying to reconnect
   * to resources that no longer exist.
   */
  public void resetCluster() {
    clusterService.reset();
  }

  public BucketService bucket() {
    return bucketService;
  }

  public StreamerService streamer() {
    return streamerService;
  }

  public DocumentService document() {
    return documentService;
  }

  public StreamBuilder newStreamer(String bucket) {
    return new StreamBuilder(bucket);
  }

  public class StreamBuilder {
    private final String bucket;
    private StreamFrom from = StreamFrom.BEGINNING;
    private StreamTo to = StreamTo.INFINITY;
    private boolean mitigateRollbacks;

    public StreamBuilder(String bucket) {
      this.bucket = requireNonNull(bucket);
      assertTrue(bucket().list().contains(bucket));
    }

    public StreamBuilder range(StreamFrom from, StreamTo to) {
      this.from = requireNonNull(from);
      this.to = requireNonNull(to);
      return this;
    }

    public StreamBuilder mitigateRollbacks() {
      this.mitigateRollbacks = true;
      return this;
    }

    public RemoteDcpStreamer start() {
      return new RemoteDcpStreamer(streamer(), streamer().start(bucket, ALL_VBUCKETS, from, to, mitigateRollbacks));
    }
  }
}
