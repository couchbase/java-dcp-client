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

import io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.INTERNAL_ROLLBACK_OPCODE;

public enum RollbackMessage {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_INT && buffer.getByte(1) == INTERNAL_ROLLBACK_OPCODE;
  }

  public static void init(ByteBuf buffer, int vbid, long seqno) {
    buffer.writeByte(MessageUtil.MAGIC_INT);
    buffer.writeByte(MessageUtil.INTERNAL_ROLLBACK_OPCODE);
    buffer.writeShort(vbid);
    buffer.writeLong(seqno);
  }

  public static int vbucket(ByteBuf buffer) {
    return buffer.getShort(2);
  }

  public static long seqno(ByteBuf buffer) {
    return buffer.getLong(4);
  }

  public static String toString(ByteBuf buffer) {
    return "Rollback [vbid: " + vbucket(buffer) + ", seqno: " + seqno(buffer) + "]";
  }

}
