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


import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

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

    /**
     * @deprecated in favor of {@link #entries(ByteBuf)}
     */
    @Deprecated
    public static int numLogEntries(final ByteBuf buffer) {
        return (MessageUtil.getContent(buffer).readableBytes() - 2) / 16;
    }

    /**
     * @deprecated in favor of {@link #entries(ByteBuf)}
     */
    @Deprecated
    public static long vbuuidEntry(final ByteBuf buffer, int index) {
        return MessageUtil.getContent(buffer).getLong(index * 16);
    }

    /**
     * @deprecated in favor of {@link #entries(ByteBuf)}
     */
    @Deprecated
    public static long seqnoEntry(final ByteBuf buffer, int index) {
        return MessageUtil.getContent(buffer).getLong(index * 16 + 8);
    }

    public static List<FailoverLogEntry> entries(final ByteBuf buffer) {
        final int numEntries = numLogEntries(buffer);
        final ByteBuf content = MessageUtil.getContent(buffer);
        final List<FailoverLogEntry> result = new ArrayList<>(numEntries);
        for (int i = 0; i < numEntries; i++) {
            final long vbuuid = content.getLong(i * 16);
            final long seqno = content.getLong(i * 16 + 8);
            result.add(new FailoverLogEntry(seqno, vbuuid));
        }
        return result;
    }

    public static String toString(final ByteBuf buffer) {
        return "FailoverLog [vbid: " + vbucket(buffer) + ", log: " + entries(buffer) + "]";
    }
}
