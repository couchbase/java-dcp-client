package com.couchbase.client.dcp.test;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.test.agent.DcpStreamer;
import com.couchbase.client.dcp.util.Version;

public class BasicRebalanceIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(BasicRebalanceIntegrationTest.class);

    private static final String COUCHBASE_DOCKER_IMAGE = "couchbase/server:5.5.0-Mar";
    private static final String NETWORK_NAME = "dcp-test-network";
    private static final String BUCKET_NAME = "beer-sample";

    private static CouchbaseContainer cbContainer1;
    private static final String kv1 = "kv1.couchbase.host";
    private static CouchbaseContainer cbContainer2;
    private static final String kv2 = "kv2.couchbase.host";
    private static AgentContainer agentContainer;
    private static RemoteAgent agent;

    @BeforeClass
    public static void setup() throws Exception {
        final Network network = Network.builder().id(NETWORK_NAME).build();
        cbContainer1 = startExposedCouchbase(network, kv1);
        cbContainer2 = startAdditionalCouchbase(network, kv2);
        agentContainer = startAgent(network);
        agent = new RemoteAgent(agentContainer);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        cbContainer1.stop();
        agentContainer.stop();
    }

    private static CouchbaseContainer startAdditionalCouchbase(Network network, String node) throws Exception {
        final int webPort = 8091;
        final String username;
        final String password;
        try (InputStream is = BasicRebalanceIntegrationTest.class.getResourceAsStream("/application.properties")) {
            Properties props = new Properties();
            props.load(is);
            username = props.getProperty("username");
            password = props.getProperty("password");
        }
        log.info("Username: " + username);
        log.info("Password: " + password);
        log.info("Nodes: " + node);
        final CouchbaseContainer couchbase = new CouchbaseContainer(COUCHBASE_DOCKER_IMAGE, node, username, password)
                .withNetwork(network).withExposedPorts(webPort);
        couchbase.start();
        Version serverVersion = couchbase.getVersion().orElse(null);
        log.info("Couchbase Server (version {}) {} running at http://localhost:{}", serverVersion, node,
                couchbase.getMappedPort(webPort));
        return couchbase;
    }

    private static CouchbaseContainer startExposedCouchbase(Network network, String node) throws Exception {
        final int webPort = 8091;
        final String username;
        final String password;

        try (InputStream is = BasicRebalanceIntegrationTest.class.getResourceAsStream("/application.properties")) {
            Properties props = new Properties();
            props.load(is);
            username = props.getProperty("username");
            password = props.getProperty("password");
        }

        log.info("Username: " + username);
        log.info("Password: " + password);
        log.info("Nodes: " + node);

        final CouchbaseContainer couchbase = new CouchbaseContainer(COUCHBASE_DOCKER_IMAGE, node, username, password)
                .withNetwork(network).withExposedPorts(webPort);
        couchbase.start();
        Version serverVersion = couchbase.getVersion().orElse(null);
        log.info("Couchbase Server (version {}) {} running at http://localhost:{}", serverVersion, node,
                couchbase.getMappedPort(webPort));
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
        final int firstCount = 7303;
        final int additionalMutations = 1024;
        assertEquals(singleton(BUCKET_NAME), agent.bucket().list());
        final String streamerId =
                agent.streamer().start(BUCKET_NAME, Collections.emptyList(), StreamFrom.BEGINNING, StreamTo.INFINITY);
        DcpStreamer.Status status = agent.streamer().awaitMutationCount(streamerId, 7303, 30, TimeUnit.SECONDS);
        assertEquals(0, status.getDeletions());
        assertEquals(0, status.getExpirations());
        assertEquals(firstCount, status.getMutations());
        cbContainer1.addServer(cbContainer2);
        cbContainer1.rebalance();
        for (int i = 0; i < additionalMutations; i++) {
            agent.document().upsert(BUCKET_NAME, "Id-" + i, "{\"Number\":" + i + "}");
        }
        status = agent.streamer().awaitMutationCount(streamerId, firstCount + additionalMutations, 60,
                TimeUnit.SECONDS);
        assertEquals(0, status.getDeletions());
        assertEquals(0, status.getExpirations());
        assertEquals(firstCount + additionalMutations, status.getMutations());
        Thread.sleep(3000);
        status = agent.streamer().status(streamerId);
        assertEquals(0, status.getDeletions());
        assertEquals(0, status.getExpirations());
        assertEquals(firstCount + additionalMutations, status.getMutations());
        agent.streamer().stop(streamerId);
    }
}
