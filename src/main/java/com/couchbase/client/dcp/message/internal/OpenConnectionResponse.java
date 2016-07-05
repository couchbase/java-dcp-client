package com.couchbase.client.dcp.message.internal;



import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.OPEN_CONNECTION_OPCODE;

public enum OpenConnectionResponse {
    ;

    /**
     * If the given buffer is a {@link OpenConnectionResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == OPEN_CONNECTION_OPCODE;
    }

}
