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
import com.github.therapi.core.annotation.Default;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class StreamerServiceImpl implements StreamerService {
  private final AtomicLong nextStreamerId = new AtomicLong(0);
  private final ConcurrentMap<String, DcpStreamer> idToStreamer = new ConcurrentHashMap<>();

  private final String username;
  private final String password;
  private final String nodes;

  public StreamerServiceImpl(@Value("${username}") String username,
                             @Value("${password}") String password,
                             @Value("${nodes}") String bootstrapHostnames) {
    this.username = username;
    this.password = password;
    this.nodes = bootstrapHostnames;
  }

  @Override
  public String start(String bucket, @Default("[]") List<Integer> vbuckets, StreamFrom from, StreamTo to, boolean mitigateRollbacks, boolean collectionAware) {
    String streamerId = "dcp-test-streamer-" + nextStreamerId.getAndIncrement();

    Client client = Client.builder()
        .bucket(bucket)
        .credentials(username, password)
        .mitigateRollbacks(mitigateRollbacks ? 100 : 0, TimeUnit.MILLISECONDS)
        .flowControl(1024 * 128)
        .seedNodes(nodes.split(","))
        .userAgent("dcp-integration-test", "0", streamerId)
        .collectionsAware(collectionAware)
        .build();

    idToStreamer.put(streamerId, new DcpStreamer(client, vbuckets, from, to));
    return streamerId;
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
  public DcpStreamer.Status awaitStreamEnd(String streamerId, long timeout, TimeUnit unit) {
    DcpStreamer.Status status = idToStreamer.get(streamerId).awaitStreamEnd(timeout, unit);
    stop(streamerId);
    return status;
  }

  @Override
  public Status awaitStateCount(String streamerId, DcpStreamer.State state, int stateCount, long timeout, TimeUnit unit) {
    return idToStreamer.get(streamerId).awaitStateCount(state, stateCount, timeout, unit);
  }

  @PreDestroy
  public void shutdown() {
    new HashSet<>(list()).forEach(this::stop);
  }

  @Override
  public Status status(String streamerId) {
    return idToStreamer.get(streamerId).status();
  }

  @Override
  public int getNumberOfPartitions(String bucket) {
    final Client client = Client.builder()
        .bucket(bucket)
        .credentials(username, password)
        .seedNodes(nodes.split(","))
        .build();

    client.connect().block();
    try {
      return client.numPartitions();
    } finally {
      client.disconnect().block();
    }
  }
}
