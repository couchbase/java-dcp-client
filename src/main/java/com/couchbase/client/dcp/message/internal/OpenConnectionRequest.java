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
package com.couchbase.client.dcp.message.internal;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import static com.couchbase.client.dcp.message.MessageUtil.OPEN_CONNECTION_OPCODE;

public enum OpenConnectionRequest {
    ;

    /**
     * If the given buffer is a {@link OpenConnectionRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == OPEN_CONNECTION_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Note that this will implicitly set the flags to "consumer".
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(OPEN_CONNECTION_OPCODE, buffer);
        // Initialize lower parts of extras flag  to producer ("1" set)
        MessageUtil.setExtras(Unpooled.buffer(8).writeInt(0).writeInt(1), buffer);
    }

    /**
     * Set the connection name on the buffer.
     */
    public static void connectionName(final ByteBuf buffer, final ByteBuf connectionName) {
        MessageUtil.setKey(connectionName, buffer);
    }

    /**
     * Returns the connection name (a slice out of the original buffer).
     */
    public static ByteBuf connectionName(final ByteBuf buffer) {
        return buffer.slice(24, buffer.getShort(2));
    }

}
