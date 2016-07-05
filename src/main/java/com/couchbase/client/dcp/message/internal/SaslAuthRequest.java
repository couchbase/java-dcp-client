package com.couchbase.client.dcp.message.internal;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_AUTH_OPCODE;

public enum SaslAuthRequest {
    ;

    /**
     * If the given buffer is a {@link SaslAuthRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == SASL_AUTH_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(SASL_AUTH_OPCODE, buffer);
    }

    /**
     * Sets the selected mechanism.
     */
    public static void mechanism(ByteBuf mechanism, ByteBuf buffer) {
        MessageUtil.setKey(mechanism, buffer);
    }

    /**
     * Sets the challenge response payload.
     */
    public static void challengeResponse(ByteBuf challengeResponse, ByteBuf buffer) {
        MessageUtil.setContent(challengeResponse, buffer);
    }
}
