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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

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
  private static AgentContainer agentContainer;
  private static RemoteAgent agent;

  @BeforeClass
  public static void setup() throws Exception {
    final Network network = Network.builder().id("dcp-test-network").build();
    final String couchbaseVersionEnvar = "COUCHBASE";
    String couchbaseVersion = System.getenv(couchbaseVersionEnvar);
    if (couchbaseVersion == null) {
      if (runningInJenkins) {
        throw new RuntimeException("Environment variable " + couchbaseVersionEnvar + " must be set when running in Jenkins." +
            " Value should be the version of the 'couchbase/server' docker image to test against.");
      } else {
        couchbaseVersion = "6.0.1";
      }
    }
    final String dockerImage = "couchbase/server:" + couchbaseVersion;
    couchbase = CouchbaseContainer.newCluster(dockerImage, network, "kv1.couchbase.host", HOST_COUCHBASE_UI_PORT);

    // Dummy bucket for authenticating management requests (required for Couchbase versions prior to 6.5).
    // Give it a replica so failover tests don't fail with complaint about losing vbuckets.
    couchbase.createBucket("default", 100, 1, true);

    agentContainer = startAgent(network);
    agent = new RemoteAgent(agentContainer);
  }

  @After
  public void resetClient() throws Exception {
    agent().resetCluster();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    stop(agentContainer, couchbase);
  }

  protected static AgentContainer startAgent(Network network) throws Exception {
    File appFile = new File("target/dcp-test-agent-0.1.0.jar");

    AgentContainer agentContainer = new AgentContainer(appFile, AGENT_UI_PORT).withNetwork(network)
        .withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger("container.agent")));
    agentContainer.start();

    return agentContainer;
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
      couchbase().createBucket(name, ramMb, replicas, true);
      return new TestBucket(name);
    }

    public TestBucket createWithoutWaiting() {
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
  }
}
