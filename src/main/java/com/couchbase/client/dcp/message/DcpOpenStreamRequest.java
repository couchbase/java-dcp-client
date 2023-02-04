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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_REQUEST_OPCODE;

public enum DcpOpenStreamRequest {
  ;

  /**
   * If the given buffer is a {@link DcpOpenStreamRequest} message.
   */
  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_STREAM_REQUEST_OPCODE;
  }

  /**
   * Initialize the buffer with all the values needed.
   * <p>
   * Initializes the complete extras needed with 0 and can be overridden through the setters available.
   * If no setters are used this message is effectively a backfill for the given vbucket.
   */
  public static void init(final ByteBuf buffer, Set<StreamFlags> flags, int vbucket) {
    MessageUtil.initRequest(DCP_STREAM_REQUEST_OPCODE, buffer);

    MessageUtil.setVbucket(vbucket, buffer);
    MessageUtil.setExtras(Unpooled
            .buffer(48)
            .writeInt(StreamFlags.encode(flags)) // flags
            .writeInt(0) // reserved
            .writeLong(0) // start sequence number
            .writeLong(0) // end sequence number
            .writeLong(0) // vbucket uuid
            .writeLong(0) // snapshot start sequence number
            .writeLong(0), // snapshot end sequence number
        buffer
    );
  }

  public static void startSeqno(final ByteBuf buffer, long seqnoStart) {
    MessageUtil.getExtras(buffer).setLong(8, seqnoStart);
  }

  public static void endSeqno(final ByteBuf buffer, long seqnoEnd) {
    MessageUtil.getExtras(buffer).setLong(16, seqnoEnd);
  }

  public static void vbuuid(final ByteBuf buffer, long uuid) {
    MessageUtil.getExtras(buffer).setLong(24, uuid);
  }

  public static void snapshotStartSeqno(final ByteBuf buffer, long snapshotSeqnoStart) {
    MessageUtil.getExtras(buffer).setLong(32, snapshotSeqnoStart);
  }

  public static void snapshotEndSeqno(final ByteBuf buffer, long snapshotSeqnoEnd) {
    MessageUtil.getExtras(buffer).setLong(40, snapshotSeqnoEnd);
  }

  public static void opaque(final ByteBuf buffer, int opaque) {
    MessageUtil.setOpaque(opaque, buffer);
  }

  public static int flagsAsInt(final ByteBuf buffer) {
    return MessageUtil.getExtras(buffer).getInt(0);
  }

  public static Set<StreamFlags> flags(final ByteBuf buffer) {
    return StreamFlags.decode(flagsAsInt(buffer));
  }

  public static void flags(final ByteBuf buffer, int flags) {
    MessageUtil.getExtras(buffer).setInt(0, flags);
  }
}
