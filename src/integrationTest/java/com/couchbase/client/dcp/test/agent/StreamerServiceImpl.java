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

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.dcp.test.agent.DcpStreamer.Status;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class StreamerServiceImpl implements StreamerService {
  private final AtomicLong nextStreamerId = new AtomicLong(0);
  private final ConcurrentMap<String, DcpStreamer> idToStreamer = new ConcurrentHashMap<>();

  private final String connectionString;
  private final String username;
  private final String password;

  public StreamerServiceImpl(
      String connectionString,
      String username,
      String password
  ) {
    this.connectionString = requireNonNull(connectionString);
    this.username = requireNonNull(username);
    this.password = requireNonNull(password);
  }

  @Override
  public String start(String bucket, List<Integer> vbuckets, StreamFrom from, StreamTo to, boolean mitigateRollbacks, boolean collectionAware) {
    String streamerId = "dcp-test-streamer-" + nextStreamerId.getAndIncrement();

    Client client = commonClientConfig(Client.builder(), bucket)
        .userAgent("dcp-integration-test", "0", streamerId)
        .mitigateRollbacks(mitigateRollbacks ? 100 : 0, TimeUnit.MILLISECONDS)
        .flowControl(1024 * 128)
        .collectionsAware(collectionAware)
        .build();

    idToStreamer.put(streamerId, new DcpStreamer(client, vbuckets, from, to));
    return streamerId;
  }

  private Client.Builder commonClientConfig(Client.Builder builder, String bucket) {
    return builder
        .connectionString(connectionString)
        .credentials(username, password)
        .bucket(bucket)
        .bootstrapTimeout(Duration.ofSeconds(60));
  }

  @Override
  public void stop(String streamerId) {
    DcpStreamer streamer = idToStreamer.remove(streamerId);
    if (streamer == null) {
      return;
    }
    streamer.stop();
  }

  @Override
  public Set<String> list() {
    return idToStreamer.keySet();
  }

  @Override
  public String get(String streamerId) {
    SessionState state = idToStreamer.get(streamerId).getSessionState();
    return new String(state.export(StateFormat.JSON), UTF_8);
  }

  @Override
  public Status awaitStreamEnd(String streamerId, long timeout, TimeUnit unit) {
    Status status = idToStreamer.get(streamerId).awaitStreamEnd(timeout, unit);
    stop(streamerId);
    return status;
  }

  @Override
  public Status awaitStateCount(String streamerId, DcpStreamer.State state, int stateCount, long timeout, TimeUnit unit) {
    return idToStreamer.get(streamerId).awaitStateCount(state, stateCount, timeout, unit);
  }

  public void shutdown() {
    new HashSet<>(list()).forEach(this::stop);
  }

  @Override
  public Status status(String streamerId) {
    return idToStreamer.get(streamerId).status();
  }

  @Override
  public int getNumberOfPartitions(String bucket) {
    final Client client = commonClientConfig(Client.builder(), bucket).build();

    client.connect().block();
    try {
      return client.numPartitions();
    } finally {
      client.disconnect().block();
    }
  }
}
