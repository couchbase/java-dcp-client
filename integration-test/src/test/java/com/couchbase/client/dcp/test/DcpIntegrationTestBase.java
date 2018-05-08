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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

public abstract class DcpIntegrationTestBase {
    private static final Logger log = LoggerFactory.getLogger(DcpIntegrationTestBase.class);

    private static final String COUCHBASE_DOCKER_IMAGE = "couchbase/server:5.5.0-beta";

    private static CouchbaseContainer couchbase;
    private static AgentContainer agentContainer;
    private static RemoteAgent agent;

    @BeforeClass
    public static void setup() throws Exception {
        final Network network = Network.builder().id("dcp-test-network").build();
        couchbase = CouchbaseContainer.newCluster(COUCHBASE_DOCKER_IMAGE, network, "kv1.couchbase.host");
        agentContainer = startAgent(network);
        agent = new RemoteAgent(agentContainer);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        stop(agentContainer, couchbase);
    }

    protected static AgentContainer startAgent(Network network) throws Exception {
        File appFile = new File("target/dcp-test-agent-0.1.0.jar");

        AgentContainer agentContainer = new AgentContainer(appFile).withNetwork(network);
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

    protected static class BucketBuilder {
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
            couchbase().createBucket(name, ramMb, replicas);
            return new TestBucket(name);
        }
    }

    protected static class TestBucket implements Closeable {
        private final String name;

        public TestBucket(String name) {
            this.name = requireNonNull(name);
        }

        protected void createDocuments(int count, String documentIdPrefix) {
            log.info("Creating {} documents in bucket '{}' with ID prefix '{}'", count, name, documentIdPrefix);
            for (int i = 0; i < count; i++) {
                final String id = documentIdPrefix + "-" + i;
                agent.document().upsert(name, id, "{\"id\":\"" + id + "\"}");
            }
        }

        protected void createDocuments(int count) {
            createDocuments(count, "document");
        }

        @Override
        public void close() throws IOException {
            couchbase().deleteBucket(name);
        }

        public RemoteAgent.StreamBuilder newStreamer() {
            return agent.newStreamer(name);
        }

        public String name() {
            return name;
        }

        public void stopPersistence() {
            couchbase().stopPersistence(name);
        }

        public void startPersistence() {
            couchbase().startPersistence(name);
        }
    }
}
