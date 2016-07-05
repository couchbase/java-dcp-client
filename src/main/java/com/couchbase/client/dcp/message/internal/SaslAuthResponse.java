package com.couchbase.client.dcp.message.internal;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_AUTH_OPCODE;

public enum SaslAuthResponse {
    ;

    /**
     * If the given buffer is a {@link SaslAuthResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == SASL_AUTH_OPCODE;
    }

    /**
     * Returns the server challenge.
     */
    public static ByteBuf challenge(final ByteBuf buffer) {
        return MessageUtil.getContent(buffer);
    }

}
