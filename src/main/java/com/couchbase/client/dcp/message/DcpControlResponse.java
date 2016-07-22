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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_CONTROL_OPCODE;

public enum DcpControlResponse {
    ;

    /**
     * If the given buffer is a {@link DcpControlResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_CONTROL_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Note that this will implicitly set the flags to "consumer".
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initResponse(DCP_CONTROL_OPCODE, buffer);
    }

    public static void key(final ByteBuf key, final ByteBuf buffer) {
        MessageUtil.setKey(key, buffer);
    }

    public static void value(final ByteBuf value, final ByteBuf buffer) {
        MessageUtil.setContent(value, buffer);
    }
}
