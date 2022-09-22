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

import static com.couchbase.client.dcp.ConnectionNameGenerator.CONNECTION_NAME_MAX_UTF8_BYTES;
import static com.couchbase.client.dcp.message.MessageUtil.OPEN_CONNECTION_OPCODE;
import static java.nio.charset.StandardCharsets.UTF_8;

public enum DcpOpenConnectionRequest {
  ;

  /**
   * If the given buffer is a {@link DcpOpenConnectionRequest} message.
   */
  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == OPEN_CONNECTION_OPCODE;
  }

  /**
   * Initialize the buffer with all the values needed.
   * <p>
   * Note that {@link OpenConnectionFlag#PRODUCER} flag is always set
   * in addition to any flags passed in.
   */
  public static void init(final ByteBuf buffer, Set<OpenConnectionFlag> flags) {
    int flagsAsInt = OpenConnectionFlag.encode(flags) | OpenConnectionFlag.PRODUCER.value();

    MessageUtil.initRequest(OPEN_CONNECTION_OPCODE, buffer);
    ByteBuf extras = Unpooled.buffer(8);
    MessageUtil.setExtras(extras.writeInt(0).writeInt(flagsAsInt), buffer);
    extras.release();
  }

  /**
   * Set the connection name on the buffer.
   */
  public static void connectionName(final ByteBuf buffer, final String connectionName) {
    int nameLen = connectionName.getBytes(UTF_8).length;
    if (nameLen > CONNECTION_NAME_MAX_UTF8_BYTES) {
      throw new IllegalArgumentException("DCP connection name must be no longer than " + CONNECTION_NAME_MAX_UTF8_BYTES + " UTF-8 bytes, but got " + nameLen + " bytes. Connection name: '" + connectionName + "'");
    }
    MessageUtil.setKey(connectionName, buffer);
  }

  /**
   * Returns the connection name (a slice out of the original buffer).
   */
  public static ByteBuf connectionName(final ByteBuf buffer) {
    return MessageUtil.getKey(buffer);
  }
}
