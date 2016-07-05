package com.couchbase.client.dcp.message.internal;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_CONTROL_OPCODE;

public enum DcpControlRequest {
    ;

    /**
     * If the given buffer is a {@link DcpControlRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_CONTROL_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Note that this will implicitly set the flags to "consumer".
     */
    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_CONTROL_OPCODE, buffer);
    }

    public static void key(final ByteBuf key, final ByteBuf buffer) {
        MessageUtil.setKey(key, buffer);
    }

    public static void value(final ByteBuf value, final ByteBuf buffer) {
        MessageUtil.setContent(value, buffer);
    }
}
