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

import com.couchbase.client.core.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.test.agent.BucketService;
import com.couchbase.client.dcp.test.agent.BucketServiceImpl;
import com.couchbase.client.dcp.test.agent.ClusterService;
import com.couchbase.client.dcp.test.agent.ClusterServiceImpl;
import com.couchbase.client.dcp.test.agent.ClusterSupplier;
import com.couchbase.client.dcp.test.agent.CollectionService;
import com.couchbase.client.dcp.test.agent.CollectionServiceImpl;
import com.couchbase.client.dcp.test.agent.DocumentService;
import com.couchbase.client.dcp.test.agent.DocumentServiceImpl;
import com.couchbase.client.dcp.test.agent.StreamerService;
import com.couchbase.client.dcp.test.agent.StreamerServiceImpl;

import java.io.Closeable;
import java.io.IOException;

import static com.couchbase.client.dcp.test.agent.StreamerService.ALL_VBUCKETS;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class RemoteAgent implements Closeable {
  static {
    ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
  }

  private final ClusterSupplier clusterSupplier;
  private final StreamerServiceImpl streamerService;
  private final DocumentService documentService;
  private final BucketService bucketService;
  private final CollectionService collectionService;
  private final ClusterService clusterService;

  public RemoteAgent(String connectionString, String username, String password) {
    this.clusterSupplier = new ClusterSupplier(connectionString, username, password);
    this.streamerService = new StreamerServiceImpl(connectionString, username, password);
    this.documentService = new DocumentServiceImpl(clusterSupplier, streamerService);
    this.bucketService = new BucketServiceImpl(clusterSupplier);
    this.collectionService = new CollectionServiceImpl(clusterSupplier);
    this.clusterService = new ClusterServiceImpl(clusterSupplier);
  }

  public StreamerService streamer() {
    return streamerService;
  }

  public DocumentService document() {
    return documentService;
  }

  public BucketService bucket() {
    return bucketService;
  }

  public CollectionService collection() {
    return collectionService;
  }

  public ClusterService cluster() {
    return clusterService;
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

  @Override
  public void close() throws IOException {
    streamerService.shutdown();
    clusterSupplier.close();
  }

  public StreamBuilder newStreamer(String bucket) {
    return new StreamBuilder(bucket);
  }

  public class StreamBuilder {
    private final String bucket;
    private StreamFrom from = StreamFrom.BEGINNING;
    private StreamTo to = StreamTo.INFINITY;
    private boolean mitigateRollbacks;
    private boolean collectionAware;

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

    public StreamBuilder collectionAware() {
      this.collectionAware = true;
      return this;
    }

    public RemoteDcpStreamer start() {
      return new RemoteDcpStreamer(
          streamer(),
          streamer().start(
              bucket,
              ALL_VBUCKETS,
              from,
              to,
              mitigateRollbacks,
              collectionAware
          )
      );
    }
  }
}
