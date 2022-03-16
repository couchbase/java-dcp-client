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

package com.couchbase.client.dcp.perfrunner;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.Deletion;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.Mutation;
import com.couchbase.client.dcp.highlevel.StreamFailure;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

/**
 * Command line driver for performance testing. See perf/README.md for more info.
 */
public class PerformanceTestDriver {

  private static class Args {
    private String connectionString;
    private long dcpMessageCount;
    private Properties settings;
    private String collectionString;
  }

  private static final LongAdder totalCompressedBytes = new LongAdder();
  private static final LongAdder totalDecompressedBytes = new LongAdder();
  private static final LongAdder totalMessageCount = new LongAdder();
  private static final LongAdder compressedMessageCount = new LongAdder();

  public static void main(String[] commandLineArguments) throws Exception {
    Args args = parseArgs(commandLineArguments);
    Client client = buildClient(args);
    runTest(client, args);
    generateReport();
  }

  private static void generateReport() throws IOException {
    File reportDir = new File("target/perf");
    if (!reportDir.exists() && !reportDir.mkdir()) {
      throw new IOException("Failed to create report directory: " + reportDir.getAbsolutePath());
    }

    try (OutputStream os = new FileOutputStream(new File(reportDir, "metrics.json"))) {
      Map<String, Object> compressionMetrics = new LinkedHashMap<>();
      compressionMetrics.put("totalMessageCount", totalMessageCount);
      compressionMetrics.put("compressedMessageCount", compressedMessageCount);
      compressionMetrics.put("totalCompressedBytes", totalCompressedBytes);
      compressionMetrics.put("totalDecompressedBytes", totalDecompressedBytes);
      compressionMetrics.put("avgCompressionRatio", totalCompressedBytes.longValue() == 0 ? "N/A" :
          new BigDecimal(totalDecompressedBytes.doubleValue() / totalCompressedBytes.doubleValue())
              .setScale(2, RoundingMode.HALF_UP));

      System.out.println("Compression metrics: " + compressionMetrics);

      new ObjectMapper().writerWithDefaultPrettyPrinter()
          .writeValue(os, singletonMap("compression", compressionMetrics));
    }
  }

  private static Args parseArgs(String[] args) throws IOException {
    Iterator<String> i = Arrays.asList(args).iterator();
    Args result = new Args();
    result.connectionString = i.next();
    result.dcpMessageCount = Long.parseLong(i.next());
    if (result.dcpMessageCount < 1) {
      throw new IllegalArgumentException("message count must be > 0");
    }
    File configFile = new File(i.next());
    if (i.hasNext()) {
      result.collectionString = i.next();
      System.out.println("Target Collections: " + result.collectionString);
    }

    Properties props = new Properties();
    props.load(new FileInputStream(configFile));
    result.settings = props;

    System.out.println("Configuration properties read from " + configFile.getAbsolutePath());
    props.list(System.out);
    System.out.println();

    return result;
  }

  private static void registerLowLevelListeners(AtomicLong remaining, CountDownLatch latch, Client client, boolean skipDecompression) {
    // Don't do anything with control events in this example
    client.controlEventHandler((flowController, event) -> {
      if (DcpSnapshotMarkerRequest.is(event)) {
        flowController.ack(event);
      }
      event.release();
    });

    client.dataEventHandler((flowController, event) -> {
      totalMessageCount.increment();

      if (!skipDecompression) {
        if (MessageUtil.isSnappyCompressed(event)) {
          compressedMessageCount.increment();
          totalCompressedBytes.add(MessageUtil.getRawContent(event).readableBytes());

          // getContent() triggers decompression, so it's important for perf test to call it.
          totalDecompressedBytes.add(MessageUtil.getContent(event).readableBytes());
        }
      }

      flowController.ack(event);
      event.release();

      if (remaining.decrementAndGet() == 0) {
        latch.countDown();
      }
    });
  }

  private static void registerHighLevelListeners(AtomicLong remaining, CountDownLatch latch, Client client) {
    client.nonBlockingListener(new DatabaseChangeListener() {
      @Override
      public void onFailure(StreamFailure streamFailure) {
        System.err.println("Stream failure for vbucket " + streamFailure.getVbucket() + "; " + streamFailure.getCause());
        streamFailure.getCause().printStackTrace();
      }

      @Override
      public void onMutation(Mutation event) {
        process(event);
      }

      @Override
      public void onDeletion(Deletion event) {
        process(event);
      }

      private void process(DocumentChange event) {
        // High-level API can't tell if the original was compressed or not, so just count the messages.
        totalMessageCount.increment();
        event.flowControlAck();

        if (remaining.decrementAndGet() == 0) {
          latch.countDown();
        }
      }
    });
  }

  private static void runTest(final Client client, Args args) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicLong remaining = new AtomicLong(args.dcpMessageCount);

    final boolean highLevelApi = Boolean.parseBoolean(args.settings.getProperty("highLevelApi", "false"));
    final boolean skipDecompression = Boolean.parseBoolean(args.settings.getProperty("skipDecompression"));

    if (highLevelApi && skipDecompression) {
      throw new IllegalArgumentException("Can't skip decompression when using high level API");
    }

    if (highLevelApi) {
      System.out.println("Using high-level API. Won't be collecting compression metrics.");
      registerHighLevelListeners(remaining, latch, client);
    } else {
      System.out.println("Using low-level API.");
      if (skipDecompression) {
        System.out.println("Skipping decompression; won't be collecting compression metrics.");
      }
      registerLowLevelListeners(remaining, latch, client, skipDecompression);
    }

    long startNanos = System.nanoTime();
    client.connect().block();
    client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).block();
    client.startStreaming().block();

    latch.await();
    System.out.println("Received at least " + args.dcpMessageCount + " messages. Done!");
    long elapsedNanos = System.nanoTime() - startNanos;

    client.disconnect().block();

    System.out.println("Shutdown complete. Receiving " + args.dcpMessageCount + " DCP events took " +
        TimeUnit.NANOSECONDS.toMillis(elapsedNanos) + " ms");
  }

  private static Client buildClient(Args args) {
    CompressionMode compressionMode = CompressionMode.valueOf(
        args.settings.getProperty("compression", CompressionMode.DISABLED.name()));

    final boolean mitigateRollbacks = Boolean.parseBoolean(args.settings.getProperty("mitigateRollbacks"));

    PerformanceTestConnectionString connectionString = new PerformanceTestConnectionString(args.connectionString);

    List<String> hostnames = new ArrayList<>();
    for (InetSocketAddress host : connectionString.hosts()) {
      final String hostAndMaybePort = host.getPort() == 0
          ? host.getHostString()
          : host.getHostString() + ":" + host.getPort();
      hostnames.add(hostAndMaybePort);
    }

    final String username = requireNonNull(connectionString.username(), "Connection string is missing username");
    final String password = requireNonNull(connectionString.password(), "Connection string is missing password");
    final Client.Builder builder = Client.builder()
        .credentials(username, password)
        .seedNodes(hostnames)
        .bucket(requireNonNull(connectionString.bucket(), "Connection string is missing bucket name"))
        .compression(compressionMode);

    if (args.collectionString != null) {
      String[] colls = args.collectionString.replace(":", ".").split(",");
      builder.collectionsAware(true).collectionNames(colls);
    }

    if (mitigateRollbacks) {
      final int KB = 1024;
      final int MB = 1024 * KB;
      final int bufferSize = 24 * MB;

      final int pollingInterval = 100;
      final TimeUnit intervalUnit = TimeUnit.MILLISECONDS;

      System.out.println("Mitigating rollbacks with flow control buffer of " + bufferSize
          + " bytes and polling interval of " + pollingInterval + " " + intervalUnit);

      builder.flowControl(bufferSize)
          .mitigateRollbacks(pollingInterval, intervalUnit);
    } else {
      System.out.println("Rollback mitigation disabled.");
    }

    return builder.build();
  }
}
