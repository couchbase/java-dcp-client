/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.message;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

import java.util.Set;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_ADD_STREAM_OPCODE;

/**
 * Sent to the consumer to tell the consumer to initiate a stream request with the producer
 */
public enum DcpAddStreamRequest {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_ADD_STREAM_OPCODE;
  }

  public static int flagsAsInt(final ByteBuf buffer) {
    return MessageUtil.getExtras(buffer).getInt(0);
  }

  public static Set<StreamFlags> flags(final ByteBuf buffer) {
    return StreamFlags.decode(flagsAsInt(buffer));
  }

  public static void init(final ByteBuf buffer) {
    MessageUtil.initRequest(DCP_ADD_STREAM_OPCODE, buffer);
    flags(buffer, 0);
  }

  public static void flags(final ByteBuf buffer, int flags) {
    ByteBuf extras = Unpooled.buffer(4);
    MessageUtil.setExtras(extras.writeInt(flags), buffer);
    extras.release();
  }
}
