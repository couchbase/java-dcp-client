/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.endpoint.util.Snappy;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.ChannelInboundHandlerAdapter;

import static com.couchbase.client.dcp.message.MessageUtil.BODY_LENGTH_OFFSET;
import static com.couchbase.client.dcp.message.MessageUtil.EXTRAS_LENGTH_OFFSET;
import static com.couchbase.client.dcp.message.MessageUtil.HEADER_SIZE;
import static com.couchbase.client.dcp.message.MessageUtil.KEY_LENGTH_OFFSET;

/**
 * Decompresses snappy-encoded values of incoming messages.
 */
public class SnappyDecoder extends ChannelInboundHandlerAdapter {
    // Data type bitmask indicating the value of a message is compressed with Snappy.
    private static final byte DATA_TYPE_SNAPPY = 0x02;

    private final Snappy snappy = new Snappy();

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        final ByteBuf origBuffer = (ByteBuf) msg;

        final byte origDataType = MessageUtil.getDataType(origBuffer);
        final boolean snappyCompressed = (origDataType & DATA_TYPE_SNAPPY) == DATA_TYPE_SNAPPY;
        if (!snappyCompressed) {
            ctx.fireChannelRead(origBuffer);
            return;
        }

        final int estimatedDecompressedLength = origBuffer.readableBytes() * 2;
        final ByteBuf decompressed = ctx.alloc().buffer(estimatedDecompressedLength);

        try {
            final int keyLength = origBuffer.getUnsignedShort(KEY_LENGTH_OFFSET);
            final short extrasLength = origBuffer.getUnsignedByte(EXTRAS_LENGTH_OFFSET);
            final int lengthWithoutValue = HEADER_SIZE + keyLength + extrasLength;

            // Copy verbatim the header, extras, and key.
            decompressed.writeBytes(origBuffer, lengthWithoutValue);

            // Copy the value, decompressing on-the-fly.
            snappy.decode(origBuffer, decompressed);

            // Patch the data type and body length fields.
            MessageUtil.setDataType((byte) (origDataType & ~DATA_TYPE_SNAPPY), decompressed);
            int decompressedValueLength = decompressed.readableBytes() - lengthWithoutValue;
            int decompressedTotalBodyLength = keyLength + extrasLength + decompressedValueLength;
            decompressed.setInt(BODY_LENGTH_OFFSET, decompressedTotalBodyLength);

        } catch (Exception ex) {
            decompressed.release();
            throw new RuntimeException("Could not decode snappy-compressed value.", ex);

        } finally {
            origBuffer.release();
            snappy.reset();
        }

        ctx.fireChannelRead(decompressed);
    }
}
