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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_FAILOVER_LOG_OPCODE;

public enum DcpFailoverLogResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_FAILOVER_LOG_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initResponse(DCP_FAILOVER_LOG_OPCODE, buffer);
    }

    public static void vbucket(final ByteBuf buffer, final short vbid) {
        MessageUtil.setVbucket(vbid, buffer);
    }

    public static short vbucket(final ByteBuf buffer) {
        int vbOffset = MessageUtil.getContent(buffer).readableBytes() - 2;
        return MessageUtil.getContent(buffer).getShort(vbOffset);
    }

    public static int numLogEntries(final ByteBuf buffer) {
        return (MessageUtil.getContent(buffer).readableBytes() - 2) / 16;
    }

    public static long vbuuidEntry(final ByteBuf buffer, int index) {
        return MessageUtil.getContent(buffer).getLong(index * 16);
    }

    public static long seqnoEntry(final ByteBuf buffer, int index) {
        return MessageUtil.getContent(buffer).getLong(index * 16 + 8);
    }

    public static String toString(final ByteBuf buffer) {
        StringBuilder sb = new StringBuilder();
        sb.append("FailoverLog [");
        sb.append("vbid: ").append(vbucket(buffer)).append(", log: [");
        int numEntries = numLogEntries(buffer);
        for (int i = 0; i < numEntries; i++) {
            sb.append("[uuid: ").append(vbuuidEntry(buffer, i)).append(", seqno: ").append(seqnoEntry(buffer, i)).append("]");
        }
        return sb.append("]]").toString();
    }
}
