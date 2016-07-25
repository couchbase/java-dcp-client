package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_BUFFER_ACK_OPCODE;

public enum DcpBufferAckRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_BUFFER_ACK_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(DCP_BUFFER_ACK_OPCODE, buffer);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }

    public static void ackBytes(final ByteBuf buffer, int bytes) {
        MessageUtil.setExtras(Unpooled.buffer(4).writeInt(bytes), buffer);
    }

}
