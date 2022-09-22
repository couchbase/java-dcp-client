/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.highlevel.internal.CollectionIdAndKey;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.HelloFeature;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.metrics.LogLevel;
import com.couchbase.client.dcp.transport.netty.DcpConnectHandler;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.json.JsonMapper;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class LoggingTracer implements Tracer {
  private static final Logger log = LoggerFactory.getLogger("com.couchbase.client.dcp.trace");

  private static final JsonMapper mapper = new JsonMapper();
  private final LogLevel level;
  private final Predicate<String> documentIdMatcher;

  /**
   * @param level level at which to log the trace messages
   * @param documentIdIsInteresting (nullable) tests a document ID and returns true
   * if events related to this document ID should be logged. Null means log all documents.
   */
  public LoggingTracer(LogLevel level, Predicate<String> documentIdIsInteresting) {
    this.level = requireNonNull(level);
    this.documentIdMatcher = documentIdIsInteresting == null ? s -> true : documentIdIsInteresting;
  }

  private boolean disabled() {
    return !level.isEnabled(log);
  }

  public void onConnectionOpen(String channel) {
    if (disabled()) {
      return;
    }

    Map<String, Object> message = new LinkedHashMap<>();
    message.put("event", "CONNECTION_OPEN");
    message.put("channel", channel);
    log(message);
  }

  public void onConnectionClose(String channel) {
    if (disabled()) {
      return;
    }

    Map<String, Object> message = new LinkedHashMap<>();
    message.put("event", "CONNECTION_CLOSE");
    message.put("channel", channel);
    log(message);
  }

  public void onStreamStart(HostAndPort address, int partition, long partitionUuid, long startSeqno, long endSeqno,
                            long snapshotStartSeqno, long snapshotEndSeqno, Map<String, Object> value) {
    if (disabled()) {
      return;
    }

    Map<String, Object> message = new LinkedHashMap<>();
    message.put("event", "STREAM_START");
    message.put("address", address.format());
    message.put("partition", partition);
    message.put("partitionUuid", partitionUuid);
    message.put("startSeqno", startSeqno);
    message.put("endSeqno", endSeqno);
    message.put("snapshotStartSeqno", snapshotStartSeqno);
    message.put("snapshotEndSeqno", snapshotEndSeqno);
    message.put("value", value);
    log(message);
  }

  public void onStreamStartFailed(HostAndPort address, int partition, String cause) {
    if (disabled()) {
      return;
    }

    Map<String, Object> message = new LinkedHashMap<>();
    message.put("event", "STREAM_START_FAILED");
    message.put("address", address.format());
    message.put("partition", partition);
    message.put("cause", cause);
    log(message);
  }

  public void onDataEvent(ByteBuf buf, Channel channel) {
    if (disabled()) {
      return;
    }

    boolean collectionsEnabled = DcpConnectHandler.getFeatures(channel)
        .contains(HelloFeature.COLLECTIONS);
    CollectionIdAndKey idAndKey = MessageUtil.getCollectionIdAndKey(buf, collectionsEnabled);

    if (!documentIdMatcher.test(idAndKey.key())) {
      return;
    }

    String type;
    if (DcpMutationMessage.is(buf)) {
      type = "mutation";
    } else if (DcpDeletionMessage.is(buf)) {
      type = "deletion";
    } else if (DcpExpirationMessage.is(buf)) {
      type = "expiration";
    } else {
      type = "unrecognized";
    }

    Map<String, Object> message = new LinkedHashMap<>();
    message.put("event", "DOCUMENT_CHANGE");
    message.put("type", type);
    message.put("id", idAndKey.key());
    message.put("collection", idAndKey.collectionId());
    message.put("partition", MessageUtil.getVbucket(buf));
    message.put("seqno", DcpMutationMessage.bySeqno(buf));
    message.put("rev", DcpMutationMessage.revisionSeqno(buf));
    message.put("cas", DcpMutationMessage.cas(buf));

    log(message);
  }

  public void onControlEvent(ByteBuf buf, Channel channel) {
    if (disabled()) {
      return;
    }

    if (!DcpStreamEndMessage.is(buf)) {
      return;
    }

    Map<String, Object> message = new LinkedHashMap<>();
    message.put("event", "STREAM_END");
    message.put("address", DcpChannel.getHostAndPort(channel).format());
    message.put("partition", MessageUtil.getVbucket(buf));
    message.put("reason", DcpStreamEndMessage.reason(buf).name());
    log(message);
  }

  private void log(Object message) {
    try {
      level.log(log, "{}", mapper.writeValueAsString(message));
    } catch (Exception e) {
      level.log(log, "{}", message);
    }
  }
}
