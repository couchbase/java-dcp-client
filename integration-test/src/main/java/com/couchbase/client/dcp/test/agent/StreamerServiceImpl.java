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
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.dcp.test.agent.DcpStreamer.Status;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.github.therapi.core.annotation.Default;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
public class StreamerServiceImpl implements StreamerService {
  private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup(16);
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
  public String start(String bucket, @Default("[]") List<Short> vbuckets, StreamFrom from, StreamTo to, boolean mitigateRollbacks) {
    String streamerId = "dcp-test-streamer-" + nextStreamerId.getAndIncrement();

    Client client = Client.configure()
        .eventLoopGroup(eventLoopGroup)
        .bucket(bucket)
        .username(username)
        .password(password)
        .mitigateRollbacks(mitigateRollbacks ? 100 : 0, TimeUnit.MILLISECONDS)
        .flowControl(1024 * 128)
        .hostnames(nodes.split(","))
        .controlParam(DcpControl.Names.ENABLE_NOOP, true)
        .connectionNameGenerator(() -> streamerId)
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
  public DcpStreamer.Status awaitStreamEnd(String streamerId, long timeout, TimeUnit unit) throws TimeoutException {
    DcpStreamer.Status status = idToStreamer.get(streamerId).awaitStreamEnd(timeout, unit);
    stop(streamerId);
    return status;
  }

  @Override
  public DcpStreamer.Status awaitMutationCount(String streamerId, int mutationCount, long timeout, TimeUnit unit) {
    DcpStreamer.Status status = idToStreamer.get(streamerId).awaitMutationCount(mutationCount, timeout, unit);
    return status;
  }

  @PreDestroy
  public void shutdown() throws InterruptedException {
    list().forEach(this::stop);
    eventLoopGroup.shutdownGracefully().await(3, TimeUnit.SECONDS);
  }

  @Override
  public Status status(String streamerId) {
    return idToStreamer.get(streamerId).status();
  }

  @Override
  public int getNumberOfPartitions(String bucket) {
    final Client client = Client.configure()
        .eventLoopGroup(eventLoopGroup)
        .bucket(bucket)
        .username(username)
        .password(password)
        .hostnames(nodes.split(","))
        .build();

    client.connect().await();
    try {
      return client.numPartitions();
    } finally {
      client.disconnect().await();
    }
  }
}
