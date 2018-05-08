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

import com.couchbase.client.dcp.test.ExecUtils.ExecResultWithExitCode;
import com.couchbase.client.dcp.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.dcp.test.ExecUtils.exec;
import static com.couchbase.client.dcp.test.ExecUtils.execOrDie;
import static java.util.Objects.requireNonNull;

public class CouchbaseContainer extends GenericContainer<CouchbaseContainer> {
    private static final Logger log = LoggerFactory.getLogger(CouchbaseContainer.class);

    private static final int CONTAINTER_WEB_UI_PORT = 8091;
    private static final int CLUSTER_RAM_MB = 1024;

    private final String username;
    private final String password;
    private final String hostname;
    private final String dockerImageName;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private volatile Optional<Version> version;

    private CouchbaseContainer(String dockerImageName, String hostname, String username, String password, int hostUiPort) {
        super(dockerImageName);
        this.dockerImageName = requireNonNull(dockerImageName);
        this.username = requireNonNull(username);
        this.password = requireNonNull(password);
        this.hostname = requireNonNull(hostname);

        withNetworkAliases(hostname);
        withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname));

        withExposedPorts(CONTAINTER_WEB_UI_PORT);
        if (hostUiPort != 0) {
            addFixedExposedPort(hostUiPort, CONTAINTER_WEB_UI_PORT);
        }
    }

    public static CouchbaseContainer newCluster(String dockerImageName, Network network, String hostname, int hostUiPort) {
        final String username;
        final String password;

        try (InputStream is = BasicRebalanceIntegrationTest.class.getResourceAsStream("/application.properties")) {
            Properties props = new Properties();
            props.load(is);
            username = props.getProperty("username");
            password = props.getProperty("password");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        log.info("Username: " + username);
        log.info("Password: " + password);

        final CouchbaseContainer couchbase = new CouchbaseContainer(dockerImageName, hostname, username, password, hostUiPort)
                .withNetwork(network);

        couchbase.start();
        couchbase.assignHostname();
        couchbase.initCluster();

        return couchbase;
    }

    private void initCluster() {
        execOrDie(this, "couchbase-cli cluster-init" +
                " --cluster " + getHostname() +
                " --cluster-username=" + username +
                " --cluster-password=" + password +
//                " --services=data,query,index" +
//                " --cluster-index-ramsize=512" +
                " --cluster-ramsize=" + CLUSTER_RAM_MB);
    }


    private static final AtomicLong nodeCounter = new AtomicLong(2);

    public CouchbaseContainer addNode() {
        return addNode("kv" + nodeCounter.getAndIncrement() + ".couchbase.host");
    }

    public CouchbaseContainer addNode(String hostname) {
        final CouchbaseContainer newNode = new CouchbaseContainer(dockerImageName, hostname, username, password, 0)
                .withNetwork(getNetwork())
                .withExposedPorts(getExposedPorts().toArray(new Integer[0]));

        newNode.start();
        serverAdd(newNode);

        return newNode;
    }

    private void serverAdd(CouchbaseContainer newNode) {
        execOrDie(this, "couchbase-cli server-add" +
                " --cluster " + getHostname() +
                " --user=" + username +
                " --password=" + password +
                " --server-add=" + newNode.hostname +
                " --server-add-username=" + username +
                " --server-add-password=" + password);
    }

    public void stopPersistence(String bucket) {
        execOrDie(this, "cbepctl localhost stop" +
                " -u " + username +
                " -p " + password +
                " -b " + bucket);
    }

    public void startPersistence(String bucket) {
        execOrDie(this, "cbepctl localhost start" +
                " -u " + username +
                " -p " + password +
                " -b " + bucket);
    }

    public void loadSampleBucket(String bucketName) {
        loadSampleBucket(bucketName, 100);
    }

    public void loadSampleBucket(String bucketName, int bucketQuotaMb) {
        Stopwatch timer = Stopwatch.createStarted();

        ExecResultWithExitCode result = exec(this, "cbdocloader" +
                " --cluster " + getHostname() + // + ":8091" +
                " --username " + username +
                " --password " + password +
                " --bucket " + bucketName +
                " --bucket-quota " + bucketQuotaMb +
                " --dataset ./opt/couchbase/samples/" + bucketName + ".zip");

        // Query and index services must be present to avoid this warning. We don't need those services.
        if (result.getExitCode() != 0 && !result.getStdout().contains("Errors occurred during the index creation phase")) {
            throw new UncheckedIOException(new IOException("Failed to load sample bucket: " + result));
        }

        log.info("Importing sample bucket took {}", timer);

        // cbimport is faster, but isn't always available, and fails when query & index services are absent
//        Stopwatch timer = Stopwatch.createStarted();
//        createBucket(bucketName, bucketQuotaMb);
//        exec(this, "cbimport2 json " +
//                " --cluster couchbase://" + getHostname() +
//                " --username " + username +
//                " --password " + password +
//                " --bucket " + bucketName +
//                " --format sample" +
//                " --dataset ./opt/couchbase/samples/beer-sample.zip");
//        log.info("Importing sample bucket with cbimport took " + timer);
//        return this;
    }

    public void createBucket(String bucketName) {
        createBucket(bucketName, 100, 0);
    }

    public void createBucket(String bucketName, int bucketQuotaMb, int replicas) {
        Stopwatch timer = Stopwatch.createStarted();

        execOrDie(this, "couchbase-cli bucket-create" +
                " --cluster " + getHostname() +
                " --username " + username +
                " --password " + password +
                " --bucket " + bucketName +
                " --bucket-ramsize " + bucketQuotaMb + "" +
                " --bucket-type couchbase " +
                " --bucket-replica " + replicas +
                " --wait");

        log.info("Creating bucket took " + timer);
    }

    public void deleteBucket(String bucketName) {
        Stopwatch timer = Stopwatch.createStarted();

        execOrDie(this, "couchbase-cli bucket-delete" +
                " --cluster " + getHostname() +
                " --username " + username +
                " --password " + password +
                " --bucket " + bucketName);

        log.info("Deleting bucket took " + timer);
    }

    public Optional<Version> getVersion() {
        if (this.version == null) {
            throw new IllegalStateException("Must start container before getting version.");
        }

        return this.version;
    }

    public void rebalance() {
        execOrDie(this, "couchbase-cli rebalance" +
                " -c " + hostname +
                " -u " + username +
                " -p " + password);
    }

    public void failover() {
        execOrDie(this, "couchbase-cli failover" +
                " --cluster " + getHostname() + ":8091" +
                " --username " + username +
                " --password " + password +
                " --server-failover " + getHostname() + ":8091");
    }

    private String getHostname() {
        return hostname;
    }

    @Override
    public void start() {
        super.start();

        try {
            this.version = VersionUtils.getVersion(this);
            Version serverVersion = getVersion().orElse(null);
            log.info("Couchbase Server (version {}) {} running at http://localhost:{}",
                    serverVersion, hostname, getMappedPort(CONTAINTER_WEB_UI_PORT));
        } catch (Exception e) {
            stop();
            throw new RuntimeException(e);
        }
    }

    /**
     * Ensures the node refers to itself by hostname instead of IP address.
     * Doesn't really matter, but it's nice to see consistent names in the web UI's server list.
     */
    private void assignHostname() {
        execOrDie(this, "curl --silent --user " + username + ":" + password +
                " http://127.0.0.1:8091/node/controller/rename --data hostname=" + hostname);
    }
}
