/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public enum MessageUtil {
    ;

    public static final int HEADER_SIZE = 24;

    public static final byte MAGIC_INT = (byte) 0x79;
    public static final byte MAGIC_REQ = (byte) 0x80;
    public static final byte MAGIC_RES = (byte) 0x81;

    public static final short KEY_LENGTH_OFFSET = 2;
    public static final short EXTRAS_LENGTH_OFFSET = 4;
    public static final short VBUCKET_OFFSET = 6;
    public static final short BODY_LENGTH_OFFSET = 8;
    public static final short OPAQUE_OFFSET = 12;
    public static final short CAS_OFFSET = 16;

    public static final byte SASL_LIST_MECHS_OPCODE = 0x20;
    public static final byte SASL_AUTH_OPCODE = 0x21;
    public static final byte SASL_STEP_OPCODE = 0x22;
    public static final byte GET_SEQNOS_OPCODE = 0x48;
    public static final byte OPEN_CONNECTION_OPCODE = 0x50;
    public static final byte DCP_ADD_STREAM_OPCODE = 0x51;
    public static final byte DCP_STREAM_CLOSE_OPCODE = 0x52;
    public static final byte DCP_STREAM_REQUEST_OPCODE = 0x53;
    public static final byte DCP_FAILOVER_LOG_OPCODE = 0x54;
    public static final byte DCP_STREAM_END_OPCODE = 0x55;
    public static final byte DCP_SNAPSHOT_MARKER_OPCODE = 0x56;
    public static final byte DCP_MUTATION_OPCODE = 0x57;
    public static final byte DCP_DELETION_OPCODE = 0x58;
    public static final byte DCP_EXPIRATION_OPCODE = 0x59;
    public static final byte DCP_FLUSH_OPCODE = 0x5a;
    public static final byte DCP_SET_VBUCKET_STATE_OPCODE = 0x5b;
    public static final byte DCP_NOOP_OPCODE = 0x5c;
    public static final byte DCP_BUFFER_ACK_OPCODE = 0x5d;
    public static final byte DCP_CONTROL_OPCODE = 0x5e;

    public static final byte INTERNAL_ROLLBACK_OPCODE = 0x01;

    /**
     * Returns true if message can be processed and false if more data is needed.
     */
    public static boolean isComplete(final ByteBuf buffer) {
        int readable = buffer.readableBytes();
        if (readable < HEADER_SIZE) {
            return false;
        }
        return readable >= (HEADER_SIZE + buffer.getInt(BODY_LENGTH_OFFSET));
    }

    /**
     * Dumps the given ByteBuf in the "wire format".
     *
     * Note that the response is undefined if a buffer with a different
     * content than the KV protocol is passed in.
     *
     * @return the String ready to be printed/logged.
     */
    public static String humanize(final ByteBuf buffer) {
        StringBuilder sb = new StringBuilder();

        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
        int bodyLength = buffer.getInt(BODY_LENGTH_OFFSET);

        sb.append("Field          (offset) (value)\n-----------------------------------\n");
        sb.append(String.format("Magic          (0)      0x%02x\n", buffer.getByte(0)));
        sb.append(String.format("Opcode         (1)      0x%02x\n", buffer.getByte(1)));
        sb.append(String.format("Key Length     (2,3)    0x%04x\n", keyLength));
        sb.append(String.format("Extras Length  (4)      0x%02x\n", extrasLength));
        sb.append(String.format("Data Type      (5)      0x%02x\n", buffer.getByte(5)));
        sb.append(String.format("VBucket        (6,7)    0x%04x\n", buffer.getShort(VBUCKET_OFFSET)));
        sb.append(String.format("Total Body     (8-11)   0x%08x\n", bodyLength));
        sb.append(String.format("Opaque         (12-15)  0x%08x\n", buffer.getInt(OPAQUE_OFFSET)));
        sb.append(String.format("CAS            (16-23)  0x%016x\n", buffer.getLong(CAS_OFFSET)));

        if (extrasLength > 0) {
            sb.append("+ Extras with " + extrasLength + " bytes\n");
        }

        if (keyLength > 0) {
            sb.append("+ Key with " + keyLength + " bytes\n");
        }

        int contentLength = bodyLength - extrasLength - keyLength;
        if (contentLength > 0) {
            sb.append("+ Content with " + contentLength + " bytes\n");
        }

        return sb.toString();
    }

    /**
     * Helper method to initialize a request with an opcode.
     */
    public static void initRequest(byte opcode, ByteBuf buffer) {
        buffer.writeByte(MessageUtil.MAGIC_REQ);
        buffer.writeByte(opcode);
        buffer.writeZero(HEADER_SIZE - 2);
    }

    /**
     * Helper method to initialize a response with an opcode.
     */
    public static void initResponse(byte opcode, ByteBuf buffer) {
        buffer.writeByte(MessageUtil.MAGIC_RES);
        buffer.writeByte(opcode);
        buffer.writeZero(HEADER_SIZE - 2);
    }

    public static void setExtras(ByteBuf extras, ByteBuf buffer) {
        byte oldExtrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        byte newExtrasLength = (byte) extras.readableBytes();
        int oldBodyLength = buffer.getInt(BODY_LENGTH_OFFSET);
        int newBodyLength = oldBodyLength - oldExtrasLength + newExtrasLength;

        buffer.setByte(EXTRAS_LENGTH_OFFSET, newExtrasLength);
        buffer.setInt(BODY_LENGTH_OFFSET, newBodyLength);

        buffer.setBytes(HEADER_SIZE, extras);
        buffer.writerIndex(HEADER_SIZE + newBodyLength);
    }

    public static ByteBuf getExtras(ByteBuf buffer) {
        return buffer.slice(HEADER_SIZE, buffer.getByte(EXTRAS_LENGTH_OFFSET));
    }

    public static void setVbucket(short vbucket, ByteBuf buffer) {
        buffer.setShort(VBUCKET_OFFSET, vbucket);
    }

    public static short getVbucket(ByteBuf buffer) {
       return buffer.getShort(VBUCKET_OFFSET);
    }

    /**
     * Helper method to set the key, update the key length and the content length.
     */
    public static void setKey(ByteBuf key, ByteBuf buffer) {
        short oldKeyLength = buffer.getShort(KEY_LENGTH_OFFSET);
        short newKeyLength = (short) key.readableBytes();
        int oldBodyLength = buffer.getInt(BODY_LENGTH_OFFSET);
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        int newBodyLength = oldBodyLength - oldKeyLength + newKeyLength;

        buffer.setShort(KEY_LENGTH_OFFSET, newKeyLength);
        buffer.setInt(BODY_LENGTH_OFFSET, newBodyLength);

        buffer.setBytes(HEADER_SIZE + extrasLength, key);
        buffer.writerIndex(HEADER_SIZE + newBodyLength);

        // todo: make sure stuff is still in order if content is there and its sliced in
        // todo: what if old key with different size is there
    }

    public static ByteBuf getKey(ByteBuf buffer) {
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
        return buffer.slice(HEADER_SIZE + extrasLength, keyLength);
    }

    /**
     * Sets the content payload of the buffer, updating the content length as well.
     */
    public static void setContent(ByteBuf content, ByteBuf buffer) {
        short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        int bodyLength = keyLength + extrasLength + content.readableBytes();

        buffer.setInt(BODY_LENGTH_OFFSET, bodyLength);
        buffer.setBytes(HEADER_SIZE + extrasLength + keyLength, content);
        buffer.writerIndex(HEADER_SIZE + bodyLength);

        // todo: what if old body with different size is there?
    }

    public static ByteBuf getContent(ByteBuf buffer) {
        short keyLength = buffer.getShort(KEY_LENGTH_OFFSET);
        byte extrasLength = buffer.getByte(EXTRAS_LENGTH_OFFSET);
        int contentLength = buffer.getInt(BODY_LENGTH_OFFSET) - keyLength - extrasLength;
        return buffer.slice(HEADER_SIZE + keyLength + extrasLength, contentLength);
    }

    public static short getStatus(ByteBuf buffer) {
        return buffer.getShort(VBUCKET_OFFSET);
    }

    public static void setOpaque(int opaque, ByteBuf buffer) {
        buffer.setInt(OPAQUE_OFFSET, opaque);
    }

    public static int getOpaque(ByteBuf buffer) {
        return buffer.getInt(OPAQUE_OFFSET);
    }

    public static long getCas(ByteBuf buffer) {
        return buffer.getLong(CAS_OFFSET);
    }

}
