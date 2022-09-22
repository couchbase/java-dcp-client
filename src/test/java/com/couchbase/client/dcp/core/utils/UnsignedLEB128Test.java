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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


public class UnsignedLEB128Test {

  /**
   * https://github.com/couchbase/kv_engine/blob/master/docs/Collections.md#leb128-encoded-examples
   */
  @Test
  void kvEngineExamples() {
    check("00", "00");
    check("01", "01");
    check("7f", "7f");
    check("80", "8001");
    check("0555", "d50a");
    check("7fff", "ffff01");
    check("bfff", "ffff02");
    check("ffff", "ffff03");
    check("8000", "808002");
    check("5555", "d5aa01");
    check("cafef00", "80debf65");
    check("cafef00d", "8de0fbd70c");
    check("ffffffff", "ffffffff0f");
  }

  @Test
  void wikipediaExample() {
    check(624485, "e58e26");
  }

  @Test
  void canHandleMaxUnsignedLong() {
    // max unsigned long, 2^64-1 == 18446744073709551615 (also known as -1)
    check("ffffffffffffffff", "ffffffffffffffffff01");
    assertEquals("ffffffffffffffffff01", hexEncode(UnsignedLEB128.encode(-1)));
  }

  @Test
  void throwsOnOverflow() {
    // One greater than max unsigned long, 2^64 == 18446744073709551616 (overflows unsigned long)
    byte[] tooBig = hexDecode("80808080808080808002");
    assertThrows(ArithmeticException.class, () -> UnsignedLEB128.decode(tooBig));
    assertThrows(ArithmeticException.class, () -> UnsignedLEB128.read(Unpooled.wrappedBuffer(tooBig)));
  }

  @Test
  void throwsOnIncompleteInput() {
    // incomplete because the last byte has high order bit set,
    // indicating there should be more bytes to follow.
    byte[] incomplete = new byte[] {(byte) 0x80};
    assertThrows(IndexOutOfBoundsException.class, () -> UnsignedLEB128.decode(incomplete));
    assertThrows(IndexOutOfBoundsException.class, () -> UnsignedLEB128.read(Unpooled.wrappedBuffer(incomplete)));
  }

  @Test
  void readerIndexUnmodifiedOnThrow() {
    ByteBuf incomplete = Unpooled.buffer().writeByte(0x80);
    assertThrows(IndexOutOfBoundsException.class, () -> UnsignedLEB128.read(incomplete));
    assertEquals(0, incomplete.readerIndex());
  }

  @Test
  void readerIndexAdvancedOnSuccess() {
    ByteBuf buffer = Unpooled.buffer().writeByte(0x00);
    assertEquals(0, UnsignedLEB128.read(buffer));
    assertEquals(1, buffer.readerIndex());
  }

  @Test
  void write() {
    final long value = Long.parseUnsignedLong("cafef00d", 16);
    ByteBuf buffer = Unpooled.buffer();
    UnsignedLEB128.write(buffer, value);
    assertEquals(5, buffer.readableBytes());
    assertEquals(value, UnsignedLEB128.read(buffer));
  }

  @Test
  void skip() {
    ByteBuf buffer = Unpooled.buffer()
        .writeByte(0x80).writeByte(0x01) // two-byte value
        .writeByte(0x00) // one-byte value
        .writeByte(0x80); // start of an incomplete value

    UnsignedLEB128.skip(buffer);
    assertEquals(2, buffer.readerIndex());

    UnsignedLEB128.skip(buffer);
    assertEquals(3, buffer.readerIndex());

    assertThrows(IndexOutOfBoundsException.class,
        () -> UnsignedLEB128.skip(buffer));
    assertEquals(3, buffer.readerIndex());
  }

  // stopgap until we upgrade to JUnit 5
  private static <T extends Throwable> T assertThrows(Class<T> expectedExceptionClass, Runnable r) {
    try {
      r.run();
      fail("expected exception " + expectedExceptionClass + " not thrown");
      throw new AssertionError("unreachable");
    } catch (Throwable t) {
      return expectedExceptionClass.cast(t);
    }
  }

  private static void check(String hexValue, String leb128Hex) {
    check(Long.parseUnsignedLong(hexValue, 16), leb128Hex);
  }

  private static void check(long value, String leb128Hex) {
    final ByteBuf buf = Unpooled.buffer();
    UnsignedLEB128.write(buf, value);
    assertEquals(leb128Hex, hexEncode(getReadableBytes(buf)));

    byte[] encoded = UnsignedLEB128.encode(value);
    assertEquals(leb128Hex, hexEncode(encoded));

    assertEquals(value, UnsignedLEB128.decode(encoded));
    assertEquals(value, UnsignedLEB128.read(buf));
  }

  private static byte[] getReadableBytes(ByteBuf buf) {
    byte[] result = new byte[buf.readableBytes()];
    buf.getBytes(buf.readerIndex(), result);
    return result;
  }

  private static String hexEncode(byte[] bytes) {
    char[] result = new char[bytes.length * 2];
    int resultIndex = 0;
    for (byte b : bytes) {
      result[resultIndex++] = toHexDigit((b >> 4) & 0xf);
      result[resultIndex++] = toHexDigit(b & 0xf);
    }
    return new String(result);
  }

  private static byte[] hexDecode(String hex) {
    if (hex.length() % 2 != 0) {
      throw new IllegalArgumentException("hex string length must be multiple of 2");
    }
    byte[] result = new byte[hex.length() / 2];
    int resultIndex = 0;
    for (int i = 0; i < hex.length(); i += 2) {
      int value = parseHexDigit(hex.charAt(i)) << 4;
      value |= parseHexDigit(hex.charAt(i + 1));
      result[resultIndex++] = (byte) value;
    }
    return result;
  }

  private static char toHexDigit(int i) {
    if (i < 0 || i > 15) {
      throw new IllegalArgumentException("hex digit must be in range [0,15] but got " + i);
    }
    return (char) (i < 10 ? '0' + i : 'a' + i - 10);
  }

  private static int parseHexDigit(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
      return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
      return c - 'A' + 10;
    }
    throw new IllegalArgumentException("bad hex digit : " + c);
  }
}
