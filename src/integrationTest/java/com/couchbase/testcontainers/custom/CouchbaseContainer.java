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

package com.couchbase.testcontainers.custom;

import com.couchbase.client.core.util.Deadline;
import com.couchbase.client.dcp.util.Version;
import com.google.common.util.concurrent.Uninterruptibles;
import com.jayway.jsonpath.JsonPath;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.dcp.core.utils.CbCollections.setOf;
import static com.couchbase.testcontainers.custom.CouchbaseService.KV;
import static com.couchbase.testcontainers.custom.CouchbaseService.QUERY;
import static com.couchbase.testcontainers.custom.ExecUtils.exec;
import static com.couchbase.testcontainers.custom.ExecUtils.execOrDie;
import static com.couchbase.testcontainers.custom.Poller.poll;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class CouchbaseContainer extends GenericContainer<CouchbaseContainer> {
  private static final Logger log = LoggerFactory.getLogger(CouchbaseContainer.class);

  private static final String hostnameSuffix = ".run" + System.currentTimeMillis() + ".host";

  private static final OkHttpClient httpClient = new OkHttpClient();

  private static final int CONTAINTER_WEB_UI_PORT = 8091;
  private static final int CLUSTER_RAM_MB = 1024;

  private static final int MGMT_PORT = 8091;
  private static final int MGMT_SSL_PORT = 18091;
  private static final int VIEW_PORT = 8092;
  private static final int VIEW_SSL_PORT = 18092;
  private static final int QUERY_PORT = 8093;
  private static final int QUERY_SSL_PORT = 18093;
  private static final int SEARCH_PORT = 8094;
  private static final int SEARCH_SSL_PORT = 18094;
  private static final int ANALYTICS_PORT = 8095;
  private static final int ANALYTICS_SSL_PORT = 18095;
  private static final int EVENTING_PORT = 8096;
  private static final int EVENTING_SSL_PORT = 18096;
  private static final int KV_PORT = 11210;
  private static final int KV_SSL_PORT = 11207;

  private final String username;
  private final String password;
  private final String hostname;
  private final String dockerImageName;

  private final Set<CouchbaseService> enabledServices = setOf(KV);

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

    exposePorts();
  }

  public static CouchbaseContainer newCluster(String dockerImageName, Network network, String hostname, int hostUiPort) {
    final String username = "Administrator";
    final String password = "password";

//    try (InputStream is = CouchbaseContainer.class.getResourceAsStream("/application.properties")) {
//      Properties props = new Properties();
//      props.load(is);
//      username = props.getProperty("username");
//      password = props.getProperty("password");
//    } catch (IOException e) {
//      throw new UncheckedIOException(e);
//    }

    log.info("Username: " + username);
    log.info("Password: " + password);

    final CouchbaseContainer couchbase = new CouchbaseContainer(dockerImageName, hostname + hostnameSuffix, username, password, hostUiPort)
        .withNetwork(network);

    couchbase.start();
    couchbase.assignHostname();
    couchbase.initCluster();
    couchbase.configureExternalPorts();

    return couchbase;
  }

  private Response doHttpRequest(
      final int port,
      final String path,
      final String method,
      final RequestBody body,
      final boolean auth
  ) {
    try {
      Request.Builder requestBuilder = new Request.Builder()
          .url("http://" + getHost() + ":" + getMappedPort(port) + path);

      if (auth) {
        requestBuilder = requestBuilder.header("Authorization", Credentials.basic(username, password));
      }

      if (body == null) {
        requestBuilder = requestBuilder.get();
      } else {
        requestBuilder = requestBuilder.method(method, body);
      }

      return httpClient.newCall(requestBuilder.build()).execute();
    } catch (Exception ex) {
      throw new RuntimeException("Could not perform request against couchbase HTTP endpoint ", ex);
    }
  }

  /**
   * Helper method to check if the response is successful and release the body if needed.
   *
   * @param response the response to check.
   * @param message the message that should be part of the exception of not successful.
   */
  private void checkSuccessfulResponse(final Response response, final String message) {
    if (!response.isSuccessful()) {
      String body = null;
      if (response.body() != null) {
        try {
          body = response.body().string();
        } catch (IOException e) {
          logger().debug("Unable to read body of response: {}", response, e);
        }
      }

      throw new IllegalStateException(message + ": " + response + ", body=" + (body == null ? "<null>" : body));
    }
  }


  /**
   * Configures the external ports for SDK access.
   * <p>
   * Since the internal ports are not accessible from outside the container, this code configures the "external"
   * hostname and services to align with the mapped ports. The SDK will pick it up and then automatically connect
   * to those ports. Note that for all services non-ssl and ssl ports are configured.
   */
  private void configureExternalPorts() {
    logger().debug("Mapping external ports to the alternate address configuration");

    final FormBody.Builder builder = new FormBody.Builder();
    builder.add("hostname", getHost());
    builder.add("mgmt", Integer.toString(getMappedPort(MGMT_PORT)));
    builder.add("mgmtSSL", Integer.toString(getMappedPort(MGMT_SSL_PORT)));

    if (enabledServices.contains(KV)) {
      builder.add("kv", Integer.toString(getMappedPort(KV_PORT)));
      builder.add("kvSSL", Integer.toString(getMappedPort(KV_SSL_PORT)));
      builder.add("capi", Integer.toString(getMappedPort(VIEW_PORT)));
      builder.add("capiSSL", Integer.toString(getMappedPort(VIEW_SSL_PORT)));
    }

    if (enabledServices.contains(QUERY)) {
      builder.add("n1ql", Integer.toString(getMappedPort(QUERY_PORT)));
      builder.add("n1qlSSL", Integer.toString(getMappedPort(QUERY_SSL_PORT)));
    }

    if (enabledServices.contains(CouchbaseService.SEARCH)) {
      builder.add("fts", Integer.toString(getMappedPort(SEARCH_PORT)));
      builder.add("ftsSSL", Integer.toString(getMappedPort(SEARCH_SSL_PORT)));
    }

    if (enabledServices.contains(CouchbaseService.ANALYTICS)) {
      builder.add("cbas", Integer.toString(getMappedPort(ANALYTICS_PORT)));
      builder.add("cbasSSL", Integer.toString(getMappedPort(ANALYTICS_SSL_PORT)));
    }

    if (enabledServices.contains(CouchbaseService.EVENTING)) {
      builder.add("eventingAdminPort", Integer.toString(getMappedPort(EVENTING_PORT)));
      builder.add("eventingSSL", Integer.toString(getMappedPort(EVENTING_SSL_PORT)));
    }

    try (Response response = doHttpRequest(
        MGMT_PORT,
        "/node/controller/setupAlternateAddresses/external",
        "PUT",
        builder.build(),
        true
    )) {

      checkSuccessfulResponse(response, "Could not configure external ports");
    }
  }

  private void exposePorts() {
    addExposedPorts(MGMT_PORT, MGMT_SSL_PORT);

    if (enabledServices.contains(KV)) {
      addExposedPorts(KV_PORT, KV_SSL_PORT);
      addExposedPorts(VIEW_PORT, VIEW_SSL_PORT);
    }
    if (enabledServices.contains(CouchbaseService.ANALYTICS)) {
      addExposedPorts(ANALYTICS_PORT, ANALYTICS_SSL_PORT);
    }
    if (enabledServices.contains(QUERY)) {
      addExposedPorts(QUERY_PORT, QUERY_SSL_PORT);
    }
    if (enabledServices.contains(CouchbaseService.SEARCH)) {
      addExposedPorts(SEARCH_PORT, SEARCH_SSL_PORT);
    }
    if (enabledServices.contains(CouchbaseService.EVENTING)) {
      addExposedPorts(EVENTING_PORT, EVENTING_SSL_PORT);
    }
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
    return addNode("kv" + nodeCounter.getAndIncrement() + hostnameSuffix);
  }

  public CouchbaseContainer addNode(String hostname) {
    final CouchbaseContainer newNode = new CouchbaseContainer(dockerImageName, hostname + hostnameSuffix, username, password, 0)
        .withNetwork(getNetwork())
        .withExposedPorts(getExposedPorts().toArray(new Integer[0]));

    newNode.start();
    serverAdd(newNode);
    newNode.configureExternalPorts();

    return newNode;
  }

  public void killMemcached() {
    execOrDie(this, "pkill -9 memcached");
  }

  private void serverAdd(CouchbaseContainer newNode) {
    execOrDie(this, "couchbase-cli server-add" +
        " --cluster " + getHostname() +
        " -u " + username + // the full name of this arg differs between server versions
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

  public void restart() {
    execOrDie(this, "sv restart couchbase-server");
    waitForReadyState();
  }

  public void waitForReadyState() {
    poll().atInterval(3, SECONDS)
        .withTimeout(2, MINUTES)
        .until(this::allNodesHealthy);
  }

  private boolean allNodesHealthy() {
    try {
      final String poolInfo = curl("pools/default");
      final List<String> nodeStatuses = JsonPath.read(poolInfo, "$.nodes[*].status");
      return nodeStatuses.stream().allMatch(status -> status.equals("healthy"));
    } catch (UncheckedIOException e) {
      return false; // This node is not responding to HTTP requests, and therefore not healthy.
    }
  }

  private String curl(String path) {
    return execOrDie(this, "curl -sS http://localhost:8091/" + path + " -u " + username + ":" + password)
        .getStdout();
  }

  public void loadSampleBucket(String bucketName) {
    loadSampleBucket(bucketName, 100);
  }

  public void loadSampleBucket(String bucketName, int bucketQuotaMb) {
    Stopwatch timer = Stopwatch.createStarted();

    ExecResult result = exec(this, "cbdocloader" +
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
    createBucket(bucketName, 100, 0, true);
  }

  public void createBucket(String bucketName, int bucketQuotaMb, int replicas, boolean wait) {
    Stopwatch timer = Stopwatch.createStarted();

    execOrDie(this, "couchbase-cli bucket-create" +
        " --cluster " + getHostname() +
        " --username " + username +
        " --password " + password +
        " --bucket " + bucketName +
        " --bucket-ramsize " + bucketQuotaMb + "" +
        " --bucket-type couchbase " +
        " --bucket-replica " + replicas +
        (wait ? " --wait" : ""));

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
    try {
      super.start();
    } catch (ContainerLaunchException e) {
      if (stackTraceAsString(e).contains("port is already allocated")) {
        e.printStackTrace();
        throw new RuntimeException("Failed to start container due to port conflict; have you stopped all other debug sessions?");
      }
    }

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

  private static String stackTraceAsString(Throwable t) {
    StringWriter w = new StringWriter();
    t.printStackTrace(new PrintWriter(w));
    return w.toString();
  }

  /**
   * Ensures the node refers to itself by hostname instead of IP address.
   * Doesn't really matter, but it's nice to see consistent names in the web UI's server list.
   */
  private void assignHostname() {
    Deadline deadline = Deadline.of(Duration.ofSeconds(10));
    while (true) {
      try {
        execOrDie(this, "curl --silent --user " + username + ":" + password +
            " http://127.0.0.1:8091/node/controller/rename --data hostname=" + hostname);
        return;
      } catch (Exception e) {
        if (deadline.exceeded()) {
          throw e;
        }
        log.warn("Failed to set hostname. Node might not be ready to handle request. Retrying...");
        sleepUninterruptibly(250, MILLISECONDS);
      }
    }
  }

  public String getConnectionString() {
    return "couchbase://" + getHost() + ":" + getMappedPort(11210);
  }
}
