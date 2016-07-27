package com.couchbase.client.dcp.message;


import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.GET_SEQNOS_OPCODE;

public enum DcpGetPartitionSeqnosRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == GET_SEQNOS_OPCODE;
    }

    public static void init(final ByteBuf buffer) {
        MessageUtil.initRequest(GET_SEQNOS_OPCODE, buffer);
    }

    public static void opaque(final ByteBuf buffer, int opaque) {
        MessageUtil.setOpaque(opaque, buffer);
    }

}
