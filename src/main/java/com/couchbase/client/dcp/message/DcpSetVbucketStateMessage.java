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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_SET_VBUCKET_STATE_OPCODE;

/**
 * This message is used during the VBucket takeover process to hand off ownership of a VBucket between two nodes.
 */
public enum DcpSetVbucketStateMessage {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_SET_VBUCKET_STATE_OPCODE;
  }

  public static int flags(final ByteBuf buffer) {
    return MessageUtil.getExtras(buffer).getInt(0);
  }

  public static boolean active(final ByteBuf buffer) {
    return (flags(buffer) & State.ACTIVE.value) == State.ACTIVE.value;
  }

  public static boolean replica(final ByteBuf buffer) {
    return (flags(buffer) & State.REPLICA.value) == State.REPLICA.value;
  }

  public static boolean pending(final ByteBuf buffer) {
    return (flags(buffer) & State.PENDING.value) == State.PENDING.value;
  }

  public static boolean dead(final ByteBuf buffer) {
    return (flags(buffer) & State.DEAD.value) == State.DEAD.value;
  }

  public static void init(final ByteBuf buffer, State state) {
    MessageUtil.initRequest(DCP_SET_VBUCKET_STATE_OPCODE, buffer);
    state(buffer, state);
  }

  public static void state(final ByteBuf buffer, State state) {
    ByteBuf extras = Unpooled.buffer(4);
    MessageUtil.setExtras(extras.writeInt(state.value), buffer);
    extras.release();
  }

  public static State state(final ByteBuf buffer) {
    return State.of(MessageUtil.getExtras(buffer).getInt(0));
  }

  public enum State {
    ACTIVE(0x01),
    REPLICA(0x02),
    PENDING(0x03),
    DEAD(0x04);

    private final int value;

    State(int value) {
      this.value = value;
    }

    static State of(int value) {
      switch (value) {
        case 0x01:
          return ACTIVE;
        case 0x02:
          return REPLICA;
        case 0x03:
          return PENDING;
        case 0x04:
          return DEAD;
        default:
          throw new IllegalArgumentException("Unknown VBucket state: " + value);
      }
    }
  }
}
