/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") {}
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

import com.couchbase.client.dcp.config.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.Map;

/**
 * Callback handler for various events that might occur while the DCP client
 * is running. Methods might be called from non-blocking IO threads,
 * and must return quickly without making blocking calls.
 */
public interface Tracer {

  /**
   * An instance that ignores all events.
   */
  Tracer NOOP = new Tracer() {
  };

  default void onConnectionOpen(String channel) {
  }

  default void onConnectionClose(String channel) {
  }

  default void onStreamStart(HostAndPort address, int partition, long partitionUuid, long startSeqno, long endSeqno, long snapshotStartSeqno, long snapshotEndSeqno, Map<String, Object> value) {
  }

  default void onStreamStartFailed(HostAndPort address, int partition, String cause) {
  }

  default void onDataEvent(ByteBuf buf, Channel channel) {
  }

  default void onControlEvent(ByteBuf buf, Channel channel) {
  }
}
