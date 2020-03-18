/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.couchbase.client.dcp.util;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.compression.DecompressionException;

/*
 * PROMINENT NOTICE: THIS CODE IS MODIFIED FROM THE ORIGINAL VERSION DISTRIBUTED
 * BY THE NETTY PROJECT.
 *
 * The following modifications have been made:
 *
 * - The class is renamed from Snappy to PatchedNettySnappyDecoder.
 * - The encoding methods are removed.
 * - The readPreamble method is made public.
 * - BUGFIX: 16-bit literal lengths and copy offsets are interpreted as unsigned values.
 * - BUGFIX: The check for "offset too large" is removed from validateOffset().
 */

/**
 * Uncompresses an input {@link ByteBuf} encoded with Snappy compression into an
 * output {@link ByteBuf}.
 * <p>
 * See <a href="https://github.com/google/snappy/blob/master/format_description.txt">snappy format</a>.
 */
public final class PatchedNettySnappyDecoder {

    // used as a return value to indicate that we haven't yet read our full preamble
    private static final int PREAMBLE_NOT_FULL = -1;
    private static final int NOT_ENOUGH_INPUT = -1;

    // constants for the tag types
    private static final int LITERAL = 0;
    private static final int COPY_1_BYTE_OFFSET = 1;
    private static final int COPY_2_BYTE_OFFSET = 2;
    private static final int COPY_4_BYTE_OFFSET = 3;

    private PatchedNettySnappyDecoder.State state = PatchedNettySnappyDecoder.State.READY;
    private byte tag;
    private int written;

    private enum State {
        READY,
        READING_PREAMBLE,
        READING_TAG,
        READING_LITERAL,
        READING_COPY
    }

    public void reset() {
        state = PatchedNettySnappyDecoder.State.READY;
        tag = 0;
        written = 0;
    }

    public void decode(ByteBuf in, ByteBuf out) {
        while (in.isReadable()) {
            switch (state) {
                case READY:
                    state = PatchedNettySnappyDecoder.State.READING_PREAMBLE;
                    // fall through
                case READING_PREAMBLE:
                    int uncompressedLength = readPreamble(in);
                    if (uncompressedLength == PREAMBLE_NOT_FULL) {
                        // We've not yet read all of the preamble, so wait until we can
                        return;
                    }
                    if (uncompressedLength == 0) {
                        // Should never happen, but it does mean we have nothing further to do
                        state = PatchedNettySnappyDecoder.State.READY;
                        return;
                    }
                    out.ensureWritable(uncompressedLength);
                    state = PatchedNettySnappyDecoder.State.READING_TAG;
                    // fall through
                case READING_TAG:
                    if (!in.isReadable()) {
                        return;
                    }
                    tag = in.readByte();
                    switch (tag & 0x03) {
                        case LITERAL:
                            state = PatchedNettySnappyDecoder.State.READING_LITERAL;
                            break;
                        case COPY_1_BYTE_OFFSET:
                        case COPY_2_BYTE_OFFSET:
                        case COPY_4_BYTE_OFFSET:
                            state = PatchedNettySnappyDecoder.State.READING_COPY;
                            break;
                    }
                    break;
                case READING_LITERAL:
                    int literalWritten = decodeLiteral(tag, in, out);
                    if (literalWritten != NOT_ENOUGH_INPUT) {
                        state = PatchedNettySnappyDecoder.State.READING_TAG;
                        written += literalWritten;
                    } else {
                        // Need to wait for more data
                        return;
                    }
                    break;
                case READING_COPY:
                    int decodeWritten;
                    switch (tag & 0x03) {
                        case COPY_1_BYTE_OFFSET:
                            decodeWritten = decodeCopyWith1ByteOffset(tag, in, out, written);
                            if (decodeWritten != NOT_ENOUGH_INPUT) {
                                state = PatchedNettySnappyDecoder.State.READING_TAG;
                                written += decodeWritten;
                            } else {
                                // Need to wait for more data
                                return;
                            }
                            break;
                        case COPY_2_BYTE_OFFSET:
                            decodeWritten = decodeCopyWith2ByteOffset(tag, in, out, written);
                            if (decodeWritten != NOT_ENOUGH_INPUT) {
                                state = PatchedNettySnappyDecoder.State.READING_TAG;
                                written += decodeWritten;
                            } else {
                                // Need to wait for more data
                                return;
                            }
                            break;
                        case COPY_4_BYTE_OFFSET:
                            decodeWritten = decodeCopyWith4ByteOffset(tag, in, out, written);
                            if (decodeWritten != NOT_ENOUGH_INPUT) {
                                state = PatchedNettySnappyDecoder.State.READING_TAG;
                                written += decodeWritten;
                            } else {
                                // Need to wait for more data
                                return;
                            }
                            break;
                    }
            }
        }
    }

    /**
     * Reads the length varint (a series of bytes, where the lower 7 bits
     * are data and the upper bit is a flag to indicate more bytes to be
     * read).
     *
     * @param in The input buffer to read the preamble from
     * @return The calculated length based on the input buffer, or 0 if
     * no preamble is able to be calculated
     */
    public static int readPreamble(ByteBuf in) {
        int length = 0;
        int byteIndex = 0;
        while (in.isReadable()) {
            int current = in.readUnsignedByte();
            length |= (current & 0x7f) << byteIndex++ * 7;
            if ((current & 0x80) == 0) {
                return length;
            }

            if (byteIndex >= 4) {
                throw new DecompressionException("Preamble is greater than 4 bytes");
            }
        }

        return 0;
    }

    /**
     * Reads a literal from the input buffer directly to the output buffer.
     * A "literal" is an uncompressed segment of data stored directly in the
     * byte stream.
     *
     * @param tag The tag that identified this segment as a literal is also
     * used to encode part of the length of the data
     * @param in The input buffer to read the literal from
     * @param out The output buffer to write the literal to
     * @return The number of bytes appended to the output buffer, or -1 to indicate "try again later"
     */
    static int decodeLiteral(byte tag, ByteBuf in, ByteBuf out) {
        in.markReaderIndex();
        int length;
        switch (tag >> 2 & 0x3F) {
            case 60:
                if (!in.isReadable()) {
                    return NOT_ENOUGH_INPUT;
                }
                length = in.readUnsignedByte();
                break;
            case 61:
                if (in.readableBytes() < 2) {
                    return NOT_ENOUGH_INPUT;
                }
                length = readSwappedUnsignedShort(in);
                break;
            case 62:
                if (in.readableBytes() < 3) {
                    return NOT_ENOUGH_INPUT;
                }
                length = ByteBufUtil.swapMedium(in.readUnsignedMedium());
                break;
            case 63:
                if (in.readableBytes() < 4) {
                    return NOT_ENOUGH_INPUT;
                }
                // This should arguably be an unsigned int, but we're unlikely to encounter literals longer than
                // Integer.MAX_VALUE in the wild.
                length = ByteBufUtil.swapInt(in.readInt());
                break;
            default:
                length = tag >> 2 & 0x3F;
        }
        length += 1;

        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return NOT_ENOUGH_INPUT;
        }

        out.writeBytes(in, length);
        return length;
    }

    /**
     * Reads a compressed reference offset and length from the supplied input
     * buffer, seeks back to the appropriate place in the input buffer and
     * writes the found data to the supplied output stream.
     *
     * @param tag The tag used to identify this as a copy is also used to encode
     * the length and part of the offset
     * @param in The input buffer to read from
     * @param out The output buffer to write to
     * @return The number of bytes appended to the output buffer, or -1 to indicate
     * "try again later"
     * @throws DecompressionException If the read offset is invalid
     */
    private static int decodeCopyWith1ByteOffset(byte tag, ByteBuf in, ByteBuf out, int writtenSoFar) {
        if (!in.isReadable()) {
            return NOT_ENOUGH_INPUT;
        }

        int initialIndex = out.writerIndex();
        int length = 4 + ((tag & 0x01c) >> 2);
        int offset = (tag & 0x0e0) << 8 >> 5 | in.readUnsignedByte();

        validateOffset(offset, writtenSoFar);

        out.markReaderIndex();
        if (offset < length) {
            int copies = length / offset;
            for (; copies > 0; copies--) {
                out.readerIndex(initialIndex - offset);
                out.readBytes(out, offset);
            }
            if (length % offset != 0) {
                out.readerIndex(initialIndex - offset);
                out.readBytes(out, length % offset);
            }
        } else {
            out.readerIndex(initialIndex - offset);
            out.readBytes(out, length);
        }
        out.resetReaderIndex();

        return length;
    }

    /**
     * Reads a compressed reference offset and length from the supplied input
     * buffer, seeks back to the appropriate place in the input buffer and
     * writes the found data to the supplied output stream.
     *
     * @param tag The tag used to identify this as a copy is also used to encode
     * the length and part of the offset
     * @param in The input buffer to read from
     * @param out The output buffer to write to
     * @return The number of bytes appended to the output buffer, or -1 to indicate
     * "try again later"
     * @throws DecompressionException If the read offset is invalid
     */
    private static int decodeCopyWith2ByteOffset(byte tag, ByteBuf in, ByteBuf out, int writtenSoFar) {
        if (in.readableBytes() < 2) {
            return NOT_ENOUGH_INPUT;
        }

        int initialIndex = out.writerIndex();
        int length = 1 + (tag >> 2 & 0x03f);

        // The Snappy format description says a copy with 2-byte offset can represent offsets
        // in the range [0..65535], indicating this is intended to be an unsigned value.
        int offset = readSwappedUnsignedShort(in);

        validateOffset(offset, writtenSoFar);

        out.markReaderIndex();
        if (offset < length) {
            int copies = length / offset;
            for (; copies > 0; copies--) {
                out.readerIndex(initialIndex - offset);
                out.readBytes(out, offset);
            }
            if (length % offset != 0) {
                out.readerIndex(initialIndex - offset);
                out.readBytes(out, length % offset);
            }
        } else {
            out.readerIndex(initialIndex - offset);
            out.readBytes(out, length);
        }
        out.resetReaderIndex();

        return length;
    }

    /**
     * Reads a compressed reference offset and length from the supplied input
     * buffer, seeks back to the appropriate place in the input buffer and
     * writes the found data to the supplied output stream.
     *
     * @param tag The tag used to identify this as a copy is also used to encode
     * the length and part of the offset
     * @param in The input buffer to read from
     * @param out The output buffer to write to
     * @return The number of bytes appended to the output buffer, or -1 to indicate
     * "try again later"
     * @throws DecompressionException If the read offset is invalid
     */
    private static int decodeCopyWith4ByteOffset(byte tag, ByteBuf in, ByteBuf out, int writtenSoFar) {
        if (in.readableBytes() < 4) {
            return NOT_ENOUGH_INPUT;
        }

        int initialIndex = out.writerIndex();
        int length = 1 + (tag >> 2 & 0x03F);

        // This should arguably be an unsigned int for consistency with the other offset types,
        // but we're unlikely to see offsets larger than Integer.MAX_VALUE in the wild.
        int offset = ByteBufUtil.swapInt(in.readInt());

        validateOffset(offset, writtenSoFar);

        out.markReaderIndex();
        if (offset < length) {
            int copies = length / offset;
            for (; copies > 0; copies--) {
                out.readerIndex(initialIndex - offset);
                out.readBytes(out, offset);
            }
            if (length % offset != 0) {
                out.readerIndex(initialIndex - offset);
                out.readBytes(out, length % offset);
            }
        } else {
            out.readerIndex(initialIndex - offset);
            out.readBytes(out, length);
        }
        out.resetReaderIndex();

        return length;
    }

    private static int readSwappedUnsignedShort(ByteBuf in) {
        return ByteBufUtil.swapShort(in.readShort()) & 0xFFFF;
    }

    /**
     * Validates that the offset extracted from a compressed reference is within
     * the permissible bounds of an offset (4 <= offset <= 32768), and does not
     * exceed the length of the chunk currently read so far.
     *
     * @param offset The offset extracted from the compressed reference
     * @param chunkSizeSoFar The number of bytes read so far from this chunk
     * @throws DecompressionException if the offset is invalid
     */
    private static void validateOffset(int offset, int chunkSizeSoFar) {
        // There is no maximum offset value.

        if (offset <= 0) {
            throw new DecompressionException("Offset is less than minimum permissible value");
        }

        if (offset > chunkSizeSoFar) {
            throw new DecompressionException("Offset exceeds size of chunk");
        }
    }

}
