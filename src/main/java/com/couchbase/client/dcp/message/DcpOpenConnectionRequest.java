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

import static com.couchbase.client.dcp.message.MessageUtil.OPEN_CONNECTION_OPCODE;

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
   * Note that this will implicitly set the flags to "consumer".
   */
  public static void init(final ByteBuf buffer) {
    MessageUtil.initRequest(OPEN_CONNECTION_OPCODE, buffer);
    ByteBuf extras = Unpooled.buffer(8);
    MessageUtil.setExtras(extras.writeInt(0).writeInt(Type.PRODUCER.value), buffer);
    extras.release();
  }

  /**
   * Set the connection name on the buffer.
   */
  public static void connectionName(final ByteBuf buffer, final String connectionName) {
    MessageUtil.setKey(connectionName, buffer);
  }

  /**
   * Returns the connection name (a slice out of the original buffer).
   */
  public static ByteBuf connectionName(final ByteBuf buffer) {
    return MessageUtil.getKey(buffer);
  }

  enum Type {
    /**
     * Consumer type of DCP connection is set then the sender of the {@link DcpOpenConnectionRequest}
     * will be a Producer
     */
    CONSUMER(0x00),
    /**
     * Producer type of DCP connection is set then the sender of the {@link DcpOpenConnectionRequest}
     * will be a Consumer
     */
    PRODUCER(0x01),
    /**
     * Notifier type of DCP connection is set then the sender of the {@link DcpOpenConnectionRequest}
     * will be a Consumer. Notifier mode is almost the same as Producer, but it relaxes some of the
     * checks when adding streams. It does not check provided sequence numbers and always streams
     * vbucket from highest sequence number.
     */
    NOTIFIER(0x02);

    private final int value;

    Type(int value) {
      this.value = value;
    }
  }
}
