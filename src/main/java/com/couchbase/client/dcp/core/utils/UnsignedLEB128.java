/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.core.utils;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;

import java.io.ByteArrayOutputStream;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;

/**
 * Encodes and decodes the unsigned LEB128 (Little Endian Base 128) format.
 * See https://en.wikipedia.org/wiki/LEB128
 */
public class UnsignedLEB128 {
  private UnsignedLEB128() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Reads an unsigned LEB128 value from the buffer. If this methods throws an exception
   * the reader index remains unchanged, otherwise the index advances past the value.
   *
   * @return the decoded value
   * @throws ArithmeticException       if the decoded value is longer than 64 bits.
   * @throws IndexOutOfBoundsException if the buffer's readable bytes do not contain a complete value.
   */
  public static long read(ByteBuf buf) {
    final int readerMark = buf.readerIndex();
    try {
      return read(buf::readByte);
    } catch (Exception e) {
      buf.readerIndex(readerMark);
      throw e;
    }
  }

  /**
   * Advances the buffer's reader index past the unsigned LEB128 value at the reader index.
   * If this methods throws an exception the reader index will be unchanged.
   *
   * @throws IndexOutOfBoundsException if the buffer's readable bytes do not contain a complete value.
   */
  public static void skip(ByteBuf buf) {
    final int readerMark = buf.readerIndex();
    try {
      while (true) {
        if ((buf.readByte() & 0x80) == 0) {
          break;
        }
      }
    } catch (Exception e) {
      buf.readerIndex(readerMark);
      throw e;
    }
  }

  /**
   * Writes the unsigned LEB128 representation of the value to the buffer.
   */
  public static void write(ByteBuf buf, long value) {
    write(value, buf::writeByte);
  }

  /**
   * Returns a byte array containing the unsigned LEB128 representation of the given value.
   */
  public static byte[] encode(long value) {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    write(value, result::write);
    return result.toByteArray();
  }

  /**
   * Given a byte array starting with an unsigned LEB128 value, returns the decoded form
   * of that value.
   *
   * @throws ArithmeticException       if the decoded value is larger than 64 bits.
   * @throws IndexOutOfBoundsException if the input does not contain a complete LEB128 value
   */
  public static long decode(byte[] bytes) {
    return read(Unpooled.wrappedBuffer(bytes));
  }

  private static long read(IntSupplier byteSource) {
    long result = 0;

    for (int i = 0; ; i++) {
      final int b = byteSource.getAsInt();
      final long low7Bits = b & 0x7f;
      final boolean done = (b & 0x80) == 0;

      // The first 9 groups of 7 bits are guaranteed to fit (9 * 7 = 63 bits).
      // That leaves room for only the low-order bit from the 10th group (which has index 9)
      if (i == 9 && (b & 0xfe) != 0) {
        throw new ArithmeticException("Value is larger than 64-bits");
      }

      result |= low7Bits << (7 * i);
      if (done) {
        return result;
      }
    }
  }

  private static void write(long value, IntConsumer byteSink) {
    while (true) {
      final int b = (int) (value & 0x7f);
      value >>>= 7;
      if (value == 0) {
        byteSink.accept(b);
        return;
      }
      byteSink.accept(b | 0x80);
    }
  }
}
