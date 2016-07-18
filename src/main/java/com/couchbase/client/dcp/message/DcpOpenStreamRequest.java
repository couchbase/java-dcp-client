package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_STREAM_REQUEST_OPCODE;

public enum DcpOpenStreamRequest {
    ;

    /**
     * If the given buffer is a {@link DcpOpenStreamRequest} message.
     */
    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_STREAM_REQUEST_OPCODE;
    }

    /**
     * Initialize the buffer with all the values needed.
     *
     * Initializes the complete extras needed with 0 and can be overridden through the setters available.
     * If no setters are used this message is effectively a backfill for the given vbucket.
     */
    public static void init(final ByteBuf buffer, short vbucket) {
        MessageUtil.initRequest(DCP_STREAM_REQUEST_OPCODE, buffer);

        MessageUtil.setVbucket(vbucket, buffer);
        MessageUtil.setExtras(Unpooled
            .buffer(48)
            .writeInt(0) // flags
            .writeInt(0) // reserved
            .writeLong(0) // start sequence number
            .writeLong(0xffffffff) // end sequence number
            .writeLong(0) // vbucket uuid
            .writeLong(0) // snapshot start sequence number
            .writeLong(0), // snapshot end sequence number
            buffer
        );
    }
}
