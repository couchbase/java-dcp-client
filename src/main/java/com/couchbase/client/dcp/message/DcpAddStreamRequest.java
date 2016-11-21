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

/**
 * Sent to the consumer to tell the consumer to initiate a stream request with the producer
 */
public enum DcpAddStreamRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_ADD_STREAM_OPCODE;
    }

    public static int flags(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getInt(0);
    }

    /**
     * Check if {@link StreamFlags#TAKEOVER} flag requested for the stream.
     */
    public static boolean takeover(final ByteBuf buffer) {
        return StreamFlags.TAKEOVER.isSet(flags(buffer));
    }

    /**
     * Check if {@link StreamFlags#DISK_ONLY} flag requested for the stream.
     */
    public static boolean diskOnly(final ByteBuf buffer) {
        return StreamFlags.DISK_ONLY.isSet(flags(buffer));
    }

    /**
     * Check if {@link StreamFlags#LATEST} flag requested for the stream.
     */
    public static boolean latest(final ByteBuf buffer) {
        return StreamFlags.LATEST.isSet(flags(buffer));
    }

    /**
     * Check if {@link StreamFlags#NO_VALUE} flag requested for the stream.
     */
    public static boolean noValue(final ByteBuf buffer) {
        return StreamFlags.NO_VALUE.isSet(flags(buffer));
    }

    /**
     * Check if {@link StreamFlags#ACTIVE_VB_ONLY} flag requested for the stream.
     */
    public static boolean activeVbucketOnly(final ByteBuf buffer) {
        return StreamFlags.ACTIVE_VB_ONLY.isSet(flags(buffer));
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_ADD_STREAM_OPCODE, buffer);
        flags(buffer, 0);
    }

    public static void flags(final ByteBuf buffer, int flags) {
        ByteBuf extras = Unpooled.buffer(4);
        MessageUtil.setExtras(extras.writeInt(flags), buffer);
        extras.release();
    }
}
