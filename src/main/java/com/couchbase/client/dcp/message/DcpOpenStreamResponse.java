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

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_REQUEST_OPCODE;
import static com.couchbase.client.dcp.message.ResponseStatus.ROLLBACK_REQUIRED;

public enum DcpOpenStreamResponse {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == DCP_STREAM_REQUEST_OPCODE;
    }

    public static short vbucket(ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public static long rollbackSeqno(ByteBuf buffer) {
        if (MessageUtil.getResponseStatus(buffer) == ROLLBACK_REQUIRED) {
            return MessageUtil.getContent(buffer).getLong(0);
        } else {
            throw new IllegalStateException("Rollback sequence number accessible only for ROLLBACK (0x23) status code");
        }
    }
}
