/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.metrics;

import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.transport.netty.DcpResponse;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.Promise;

import static com.couchbase.client.core.logging.CouchbaseLogLevel.DEBUG;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_DELETION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_EXPIRATION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_MUTATION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_END_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.getOpcode;
import static com.couchbase.client.dcp.message.MessageUtil.getShortOpcodeName;

public class DcpChannelMetrics {
  private final MetricsContext ctx;

  private final ActionCounter connect;
  private final ActionCounter disconnect;

  // Cache the most common server request counters
  private final EventCounter mutation;
  private final EventCounter deletion;
  private final EventCounter expiration;
  private final EventCounter snapshot;

  private final EventCounter bytesRead;

  // Cache the counter associated with the previous client request in order to avoid
  // counter lookups during bursts of persistence polling requests.
  private int prevDcpRequestOpcode;
  private ActionCounter prevDcpRequestCounter;

  public DcpChannelMetrics(MetricsContext ctx) {
    this.ctx = ctx;
    this.connect = ctx.newActionCounter("connect").build();
    this.disconnect = ctx.newActionCounter("disconnect").build();

    this.mutation = serverRequestCounter(DCP_MUTATION_OPCODE).build();
    this.deletion = serverRequestCounter(DCP_DELETION_OPCODE).build();
    this.expiration = serverRequestCounter(DCP_EXPIRATION_OPCODE).build();
    this.snapshot = serverRequestCounter(DCP_SNAPSHOT_MARKER_OPCODE).build();

    this.bytesRead = ctx.newEventCounter("bytes.read").logLevel(null).build();
  }

  public <V, F extends Future<V>> F trackConnect(F future) {
    return connect.track(future);
  }

  public <V, F extends Future<V>> F trackDisconnect(F future) {
    return disconnect.track(future);
  }

  public void trackDcpRequest(Promise<DcpResponse> promise, ByteBuf request) {
    final int opcode = getOpcode(request);
    if (opcode != prevDcpRequestOpcode || prevDcpRequestCounter == null) {
      prevDcpRequestOpcode = opcode;
      prevDcpRequestCounter = ctx.newActionCounter("client.request")
          .tag("opcode", getShortOpcodeName(opcode))
          .logLevel(DEBUG)
          .build();
    }
    prevDcpRequestCounter.track(promise, dcpResponse -> {
      ResponseStatus status = dcpResponse.status();
      return status.isSuccess() ? null : status.symbolicName();
    });
  }

  public void incrementBytesRead(long bytes) {
    bytesRead.increment(bytes);
  }

  public void incrementDeadConnections() {
    ctx.newEventCounter("dead.connection")
        .build()
        .increment();
  }

  public void recordServerRequest(ByteBuf message) {
    final int opcode = MessageUtil.getOpcode(message);
    switch (opcode) {
      case DCP_MUTATION_OPCODE:
        mutation.increment();
        return;
      case DCP_SNAPSHOT_MARKER_OPCODE:
        snapshot.increment();
        return;
      case DCP_DELETION_OPCODE:
        deletion.increment();
        return;
      case DCP_EXPIRATION_OPCODE:
        expiration.increment();
        return;
      case DCP_STREAM_END_OPCODE:
        recordStreamEnd(message);
        return;
      default:
        serverRequestCounter(opcode).build().increment();
    }
  }

  private void recordStreamEnd(ByteBuf message) {
    // different metric name because some metric consumers need all metrics with the same name to have
    // the same tag keys, and we don't want to add "reason" to all the server request counters.
    ctx.newEventCounter("stream.end")
        .tag("reason", DcpStreamEndMessage.getReasonAsString(message))
        .logLevel(DEBUG)
        .build()
        .increment();
  }

  private EventCounter.Builder serverRequestCounter(int opcode) {
    return ctx.newEventCounter("server.request")
        .tag("opcode", getShortOpcodeName(opcode))
        .logLevel(null);
  }
}
