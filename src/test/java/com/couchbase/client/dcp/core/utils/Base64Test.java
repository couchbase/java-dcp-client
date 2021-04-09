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
package com.couchbase.client.dcp.core.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Base64 encoding and decoding based on the rfc4648 test vectors.
 *
 * @author Michael Nitschinger
 * @since 1.2.5
 */
public class Base64Test {

  @Test
  void shouldEncodeVectors() {
    assertEquals("", Base64.encode("".getBytes()));
    assertEquals("Zg==", Base64.encode("f".getBytes()));
    assertEquals("Zm8=", Base64.encode("fo".getBytes()));
    assertEquals("Zm9v", Base64.encode("foo".getBytes()));
    assertEquals("Zm9vYg==", Base64.encode("foob".getBytes()));
    assertEquals("Zm9vYmE=", Base64.encode("fooba".getBytes()));
    assertEquals("Zm9vYmFy", Base64.encode("foobar".getBytes()));
  }

  @Test
  void shouldDecodeVectors() {
    assertArrayEquals("".getBytes(), Base64.decode(""));
    assertArrayEquals("f".getBytes(), Base64.decode("Zg=="));
    assertArrayEquals("fo".getBytes(), Base64.decode("Zm8="));
    assertArrayEquals("foo".getBytes(), Base64.decode("Zm9v"));
    assertArrayEquals("foob".getBytes(), Base64.decode("Zm9vYg=="));
    assertArrayEquals("fooba".getBytes(), Base64.decode("Zm9vYmE="));
    assertArrayEquals("foobar".getBytes(), Base64.decode("Zm9vYmFy"));
  }

}
