package com.couchbase.client.dcp.message.internal;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_LIST_MECHS_OPCODE;

public enum SaslListMechsResponse {
    ;

    /**
     * If the given buffer is a {@link SaslListMechsResponse} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == SASL_LIST_MECHS_OPCODE;
    }

    /**
     * Extracts the supported SASL mechanisms as a string array.
     *
     * @param buffer the buffer to extract from.
     * @return the array of supported mechs, or an empty array if none found.
     */
    public static String[] supportedMechs(final ByteBuf buffer) {
        int bodyLength = buffer.getInt(MessageUtil.BODY_LENGTH_OFFSET);
        ByteBuf contentSlice = buffer.slice(MessageUtil.HEADER_SIZE, bodyLength);
        String content = contentSlice.toString(CharsetUtil.UTF_8);
        if (content == null) {
            return new String[] {};
        }
        return content.split(" ");
    }

}
