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
import com.couchbase.client.dcp.test.agent.DcpStreamer;
import com.couchbase.client.dcp.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class BasicStreamingIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(BasicStreamingIntegrationTest.class);

    private static final String COUCHBASE_DOCKER_IMAGE = "couchbase/server:5.5.0-Mar";

    private static CouchbaseContainer couchbaseContainer;
    private static AgentContainer agentContainer;
    private static RemoteAgent agent;

    @BeforeClass
    public static void setup() throws Exception {
        final Network network = Network.builder().id("dcp-test-network").build();

        couchbaseContainer = startCouchbase(network);
        agentContainer = startAgent(network);

        agent = new RemoteAgent(agentContainer);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        couchbaseContainer.stop();
        agentContainer.stop();
    }

    private static CouchbaseContainer startCouchbase(Network network) throws Exception {
        final String username;
        final String password;
        final List<String> nodes;
        final String firstNode;

        try (InputStream is = BasicStreamingIntegrationTest.class.getResourceAsStream("/application.properties")) {
            Properties props = new Properties();
            props.load(is);
            username = props.getProperty("username");
            password = props.getProperty("password");
            nodes = Arrays.stream(props.getProperty("nodes").split(","))
                    .map(String::trim)
                    .collect(toList());

            firstNode = nodes.get(0);
        }

        log.info("Username: " + username);
        log.info("Password: " + password);
        log.info("Nodes: " + nodes);

        final CouchbaseContainer couchbase = new CouchbaseContainer(
                COUCHBASE_DOCKER_IMAGE, firstNode, username, password)
                .withNetwork(network)
                .withExposedPorts(8091);

        couchbase.start();

        Version serverVersion = couchbase.getVersion().orElse(null);

        log.info("Couchbase Server (version {}) {} running at http://localhost:{}",
                serverVersion, firstNode, couchbase.getMappedPort(8091));

        couchbase.initCluster(1024);
        couchbase.loadSampleBucket("beer-sample", 100);

        return couchbase;
    }

    private static AgentContainer startAgent(Network network) throws Exception {
        File appFile = new File("target/dcp-test-agent-0.1.0.jar");

        AgentContainer agentContainer = new AgentContainer(appFile).withNetwork(network);
        agentContainer.start();

        return agentContainer;
    }

    @Test
    public void beerSampleMutationCount() throws Exception {
        assertEquals(singleton("beer-sample"), agent.bucket().list());

        final String streamerId = agent.streamer().start("beer-sample", Collections.emptyList(), StreamFrom.BEGINNING, StreamTo.NOW);
        final DcpStreamer.Status status = agent.streamer().awaitStreamEnd(streamerId, 30, TimeUnit.SECONDS);

        assertEquals(0, status.getDeletions());
        assertEquals(0, status.getExpirations());
        assertEquals(7303, status.getMutations());
    }
}
