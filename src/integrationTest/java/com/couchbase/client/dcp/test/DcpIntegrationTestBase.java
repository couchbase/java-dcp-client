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

import com.couchbase.client.dcp.test.agent.DcpStreamer;
import com.couchbase.client.dcp.util.Version;
import com.couchbase.testcontainers.custom.CouchbaseContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.junit.Assume.assumeFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class DcpIntegrationTestBase {
  private static final Logger log = LoggerFactory.getLogger(DcpIntegrationTestBase.class);

  private static final boolean runningInJenkins = System.getenv("JENKINS_URL") != null;

  // Use dynamic ports in CI environment to avoid port conflicts
  private static final boolean DYNAMIC_PORTS = runningInJenkins;

  // Supply a non-zero value to use a fixed port for Couchbase web UI.
  private static final int HOST_COUCHBASE_UI_PORT = DYNAMIC_PORTS ? 0 : 8891;

  // Supply a non-zero value to use a fixed port for Agent web UI.
  private static final int AGENT_UI_PORT = DYNAMIC_PORTS ? 0 : 8880;

  private static CouchbaseContainer couchbase;
  private static RemoteAgent agent;

  @BeforeAll
  public static void setup() throws Exception {
    final Network network = Network.builder().id("dcp-test-network").build();
    final String couchbaseVersionEnvar = "COUCHBASE";
    String couchbaseVersion = System.getenv(couchbaseVersionEnvar);
    if (couchbaseVersion == null) {
      if (runningInJenkins) {
        throw new RuntimeException("Environment variable " + couchbaseVersionEnvar + " must be set when running in Jenkins." +
            " Value should be the version of the 'couchbase/server' docker image to test against.");
      } else {
        couchbaseVersion = "7.2.3";
      }
    }
    //Use private docker repo for build-numbered images
    String containerRegistry = System.getenv("CONTAINER_REGISTRY");
    //Check if using an internal server build image (ie. 6.5.0-4960), or a released build (ie. 6.5.0)
    final String dockerImage = couchbaseVersion.matches("\\d.\\d.\\d-\\d{4}") ? containerRegistry + "/couchbase/server-internal:" + couchbaseVersion : "couchbase/server:" + couchbaseVersion;
    couchbase = CouchbaseContainer.newCluster(dockerImage, network, "kv1", HOST_COUCHBASE_UI_PORT);

    String connectionString = couchbase.getConnectionString();
    agent = new RemoteAgent(connectionString, "Administrator", "password");
  }

  @AfterEach
  public void resetClient() throws Exception {
    agent().resetCluster();
  }

  @AfterAll
  public static void cleanup() throws Exception {
    stop(couchbase);
  }


  protected static CouchbaseContainer couchbase() {
    return couchbase;
  }

  protected RemoteAgent agent() {
    return agent;
  }

  protected static void assertStatus(DcpStreamer.Status status, long expectedMutations, long expectedDeletions, long expectedExpirations) {
    assertEquals(expectedDeletions, status.getDeletions());
    assertEquals(expectedExpirations, status.getExpirations());
    assertEquals(expectedMutations, status.getMutations());
  }

  protected static void assertStatus(DcpStreamer.Status status, long expectedMutations,
                                     long expectedDeletions, long expectedExpirations,
                                     long expectedScopeCreations, long expectedScopeDrops,
                                     long expectedCollectionCreations, long expectedCollectionDrops, long expectedCollectionsFlushed) {
    assertEquals(expectedDeletions, status.getDeletions());
    assertEquals(expectedExpirations, status.getExpirations());
    assertEquals(expectedMutations, status.getMutations());
    assertEquals(expectedScopeCreations, status.getScopeCreations());
    assertEquals(expectedScopeDrops, status.getScopeDrops());
    assertEquals(expectedCollectionCreations, status.getCollectionCreations());
    assertEquals(expectedCollectionDrops, status.getCollectionDrops());
  }

  protected RemoteAgent.StreamBuilder newStreamer(String bucket) {
    return agent.newStreamer(bucket);
  }

  protected static void stop(GenericContainer first, GenericContainer... others) {
    if (first != null) {
      first.stop();
    }
    for (GenericContainer c : others) {
      if (c != null) {
        c.stop();
      }
    }
  }

  private static final AtomicLong bucketCounter = new AtomicLong();

  protected BucketBuilder newBucket() {
    return newBucket("test-" + bucketCounter.getAndIncrement());
  }

  protected BucketBuilder newBucket(String name) {
    return new BucketBuilder(name);
  }

  protected class BucketBuilder {
    private final String name;
    private int ramMb = 100;
    private int replicas = 0;

    public BucketBuilder(String name) {
      this.name = requireNonNull(name);
    }

    public BucketBuilder ramMb(int ramMb) {
      this.ramMb = ramMb;
      return this;
    }

    public BucketBuilder replicas(int replicas) {
      this.replicas = replicas;
      return this;
    }

    public TestBucket create() {
      agent.bucket().create(name, ramMb, replicas, true);
      return new TestBucket(name);
    }

    public TestBucket createWithoutWaiting() {
      // Tests calling this method require that it
      // MUST NOT wait for the bucket to be ready / fully created.
      couchbase().createBucket(name, ramMb, replicas, false);
      return new TestBucket(name);
    }
  }

  protected class TestBucket implements Closeable {
    private final String name;

    public TestBucket(String name) {
      this.name = requireNonNull(name);
    }

    protected Set<String> createOneDocumentInEachVbucket(String documentIdPrefix) {
      log.info("Creating one document in each vbucket of bucket '{}' with ID prefix '{}'", name, documentIdPrefix);
      return agent.document().upsertOneDocumentToEachVbucket(name, documentIdPrefix);
    }

    @Override
    public void close() throws IOException {
      agent().resetCluster(); // so it doesn't complain about missing bucket
      couchbase().deleteBucket(name);
    }

    public RemoteAgent.StreamBuilder newStreamer() {
      return agent.newStreamer(name);
    }

    public String name() {
      return name;
    }

    public void stopPersistence() {
      Version version = couchbase().getVersion().orElseThrow(() -> new RuntimeException("missing couchbase version"));
      assumeFalse("Skipping this test against Couchbase 6.5.0 because of MB-36904: 'cbepctl stop' command hangs when waiting to confirm Flusher is stopped on cluster with 0 mutations",
          version.equals(new Version(6, 5, 0)));

      couchbase().stopPersistence(name);
    }

    public void startPersistence() {
      couchbase().startPersistence(name);
    }

    protected List<String> createScopes(int scopes, String scopeIdPrefix) {
      log.info("Creating {} scopes, on bucket {}", scopes, name);
      return agent().collection().createScopesWithPrefix(name, scopeIdPrefix, scopes);
    }

    protected List<String> createCollections(int collections, String collectionsIdPrefix, String scope) {
      log.info("Creating {} collections, on scope {}, and bucket {}", collections, scope, name);
      return agent().collection().createCollectionsWithPrefix(name, scope, collectionsIdPrefix, collections);
    }

    protected void deleteScope(List<String> scopes) {
      log.info("Dropping scopes");
      agent().collection().deleteScopes(scopes, name);
    }

    protected void deleteCollections(List<String> collections, String scope) {
      log.info("Removing collections from scope {}", scope);
      agent().collection().deleteCollections(collections, scope, name);
    }

    protected Map<String, String> upsertOneDocumentToEachCollection(List<String> collections, String documentPrefix, String scope) {
      log.info("Creating one document in each collection, on scope {}, and bucket {}", scope, name);
      return agent().document().upsertOneDocumentToEachCollection(name, scope, collections, documentPrefix);
    }
  }
}
