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

import java.util.Set;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE;

public enum DcpSnapshotMarkerRequest {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_SNAPSHOT_MARKER_OPCODE;
  }

  public static Set<SnapshotMarkerFlag> flags(final ByteBuf buffer) {
    return SnapshotMarkerFlag.decode(flagsAsInt(buffer));
  }

  public static int flagsAsInt(final ByteBuf buffer) {
    return MessageUtil.getExtras(buffer).getInt(16);
  }

  public static long startSeqno(final ByteBuf buffer) {
    return MessageUtil.getExtras(buffer).getLong(0);
  }

  public static long endSeqno(final ByteBuf buffer) {
    return MessageUtil.getExtras(buffer).getLong(8);
  }

  public static String toString(final ByteBuf buffer) {
    return "SnapshotMarker [vbid: " + partition(buffer)
        + ", flags: " + String.format("0x%02x", flagsAsInt(buffer))
        + ", start: " + startSeqno(buffer)
        + ", end: " + endSeqno(buffer)
        + "]";
  }

  public static int partition(final ByteBuf buffer) {
    return MessageUtil.getVbucket(buffer);
  }
}
