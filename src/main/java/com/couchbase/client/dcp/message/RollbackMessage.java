package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.INTERNAL_ROLLBACK_OPCODE;

public enum RollbackMessage {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_INT && buffer.getByte(1) == INTERNAL_ROLLBACK_OPCODE;
    }

    public static void init(ByteBuf buffer, short vbid, long seqno) {
        buffer.writeByte(MessageUtil.MAGIC_INT);
        buffer.writeByte(MessageUtil.INTERNAL_ROLLBACK_OPCODE);
        buffer.writeShort(vbid);
        buffer.writeLong(seqno);
    }

    public static short vbucket(ByteBuf buffer) {
        return buffer.getShort(2);
    }
    public static long seqno(ByteBuf buffer) {
        return buffer.getLong(4);
    }

    public static String toString(ByteBuf buffer) {
        return "Rollback [vbid: " + vbucket(buffer) + ", seqno: " + seqno(buffer) + "]";
    }

}
