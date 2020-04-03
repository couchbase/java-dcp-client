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

import io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_SYSTEM_EVENT_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.getExtras;

public class DcpSystemEventRequest {
  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_SYSTEM_EVENT_OPCODE;
  }

  public static long getSeqno(final ByteBuf buffer) {
    return getExtras(buffer).readLong();
  }

  public static int getId(final ByteBuf buffer) {
    return getExtras(buffer).skipBytes(8).readInt();
  }

  public static int getVersion(final ByteBuf buffer) {
    return getExtras(buffer).skipBytes(8).skipBytes(4).readUnsignedByte();
  }
}
