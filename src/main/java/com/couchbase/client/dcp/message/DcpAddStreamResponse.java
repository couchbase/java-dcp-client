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

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_ADD_STREAM_OPCODE;

public enum DcpAddStreamResponse {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_ADD_STREAM_OPCODE;
  }

  public static void init(final ByteBuf buffer, int opaque) {
    MessageUtil.initResponse(DCP_ADD_STREAM_OPCODE, buffer);
    opaque(buffer, opaque);
  }

  /**
   * The opaque field contains the opaque value used by messages passing for that VBucket.
   */
  public static int opaque(final ByteBuf buffer) {
    return MessageUtil.getExtras(buffer).getInt(0);
  }

  public static void opaque(final ByteBuf buffer, int opaque) {
    ByteBuf extras = Unpooled.buffer(4);
    MessageUtil.setExtras(extras.writeInt(opaque), buffer);
    extras.release();
  }
}
