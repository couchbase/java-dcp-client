package com.couchbase.client.dcp.message.internal;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_AUTH_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.SASL_STEP_OPCODE;

public enum SaslStepResponse {
    ;

    /**
     * If the given buffer is a {@link SaslStepResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == SASL_STEP_OPCODE;
    }

}
