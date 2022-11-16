/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp.message;

import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.dcp.transport.netty.DcpConnectHandler;

import java.util.Arrays;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;

public enum HelloFeature {
  // For feature definitions see https://github.com/couchbase/kv_engine/blob/master/docs/BinaryProtocol.md#0x1f-helo

  DATATYPE(0x0001),
  TLS(0x0002),
  TCP_NODELAY(0x0003),
  MUTATION_SEQNO(0x0004),
  TCP_DELAY(0x0005),
  XATTR(0x0006),
  XERROR(0x0007),
  SELECT_BUCKET(0x0008),
  // 0x0009 is undefined
  SNAPPY(0x000a),
  JSON(0x000b),
  DUPLEX(0x000c),
  CLUSTERMAP_CHANGE_NOTIFICATION(0x000d),
  UNORDERED_EXECUTION(0x000e),
  TRACING(0x000f),
  ALT_REQUEST(0x0010),
  SYNC_REPLICATION(0x0011),
  COLLECTIONS(0x0012),
  OPEN_TRACING(0x0013),
  PRESERVE_TTL(0x0014),
  VATTR(0x0015);

  private final int code;

  private static final Map<Integer, HelloFeature> codeToFeature = unmodifiableMap(
      Arrays.stream(values())
          .collect(toMap(HelloFeature::code, f -> f)));

  HelloFeature(int code) {
    if (code < 0 || code > 0xffff) {
      throw new IllegalArgumentException("code doesn't fit in 2 bytes");
    }
    this.code = code;
  }

  public boolean isEnabled(Channel channel) {
    return DcpConnectHandler.getFeatures(channel).contains(this);
  }

  public boolean isEnabled(ChannelHandlerContext ctx) {
    return isEnabled(ctx.channel());
  }

  public int code() {
    return code;
  }

  public static HelloFeature forCode(int code) {
    HelloFeature f = codeToFeature.get(code);
    if (f == null) {
      throw new IllegalArgumentException("Unrecognized feature code: " + code);
    }
    return f;
  }

  @Override
  public String toString() {
    return String.format("0x%04x (%s)", code(), name());
  }
}
