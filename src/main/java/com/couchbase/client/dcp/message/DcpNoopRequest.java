package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_NOOP_OPCODE;

public enum DcpNoopRequest {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_NOOP_OPCODE;
    }


}
