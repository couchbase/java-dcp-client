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
        MessageUtil.setExtras(Unpooled.buffer(8).writeLong(0), buffer);
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
