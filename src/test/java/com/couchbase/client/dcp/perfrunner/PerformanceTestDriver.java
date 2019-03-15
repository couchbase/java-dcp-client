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
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

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

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

/**
 * Command line driver for performance testing. See perf/README.md for more info.
 */
class PerformanceTestDriver {

  private static class Args {
    private String connectionString;
    private int dcpMessageCount;
    private Properties settings;
  }

  private static final AtomicLong totalCompressedBytes = new AtomicLong();
  private static final AtomicLong totalDecompressedBytes = new AtomicLong();
  private static final AtomicLong totalMessageCount = new AtomicLong();
  private static final AtomicLong compressedMessageCount = new AtomicLong();

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
    result.dcpMessageCount = Integer.parseInt(i.next());
    File configFile = new File(i.next());
    Properties props = new Properties();
    props.load(new FileInputStream(configFile));
    result.settings = props;

    System.out.println("Configuration properties read from " + configFile.getAbsolutePath());
    props.list(System.out);
    System.out.println();

    return result;
  }

  private static void runTest(final Client client, Args args) throws InterruptedException {
    // Don't do anything with control events in this example
    client.controlEventHandler(new ControlEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpSnapshotMarkerRequest.is(event)) {
          flowController.ack(event);
        }
        event.release();
      }
    });

    final CountDownLatch latch = new CountDownLatch(args.dcpMessageCount);

    client.dataEventHandler(new DataEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        totalMessageCount.incrementAndGet();

        if (MessageUtil.isSnappyCompressed(event)) {
          compressedMessageCount.incrementAndGet();
          totalCompressedBytes.addAndGet(MessageUtil.getRawContent(event).readableBytes());

          // getContent() triggers decompression, so it's important for perf test to call it.
          totalDecompressedBytes.addAndGet(MessageUtil.getContent(event).readableBytes());
        }

        latch.countDown();
        flowController.ack(event);
        event.release();
      }
    });

    long startNanos = System.nanoTime();
    client.connect().await();
    client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).await();
    client.startStreaming().await();

    latch.await();
    System.out.println("Received at least " + args.dcpMessageCount + " messages. Done!");
    long elapsedNanos = System.nanoTime() - startNanos;

    client.disconnect().await();

    System.out.println("Shutdown complete. Receiving " + args.dcpMessageCount + " DCP events took " +
        TimeUnit.NANOSECONDS.toMillis(elapsedNanos) + " ms");
  }

  private static Client buildClient(Args args) throws IOException {
    CompressionMode compressionMode = CompressionMode.valueOf(
        args.settings.getProperty("compression", CompressionMode.DISABLED.name()));

    final boolean mitigateRollbacks = Boolean.parseBoolean(args.settings.getProperty("mitigateRollbacks"));

    PerformanceTestConnectionString connectionString = new PerformanceTestConnectionString(args.connectionString);

    List<String> hostnames = new ArrayList<>();
    for (InetSocketAddress host : connectionString.hosts()) {
      if (host.getPort() != 0) {
        throw new IllegalArgumentException("Connection string must not specify port");
      }
      hostnames.add(host.getHostName());
    }

    final Client.Builder builder = Client.configure()
        .username(requireNonNull(connectionString.username(), "Connection string is missing username"))
        .password(requireNonNull(connectionString.password(), "Connection string is missing password"))
        .hostnames(hostnames)
        .bucket(requireNonNull(connectionString.bucket(), "Connection string is missing bucket name"))
        .compression(compressionMode);

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
