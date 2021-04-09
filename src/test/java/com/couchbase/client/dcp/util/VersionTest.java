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
package com.couchbase.client.dcp.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VersionTest {

  @Test
  void shouldParseMajorMinorPatch() {
    Version version = Version.parseVersion("1.2.3");
    assertEquals("1.2.3", version.toString());
    assertEquals(1, version.major());
    assertEquals(2, version.minor());
    assertEquals(3, version.patch());
  }

  @Test
  void shouldParseMajorMinor() {
    Version version = Version.parseVersion("1.2");
    assertEquals("1.2.0", version.toString());
    assertEquals(1, version.major());
    assertEquals(2, version.minor());
    assertEquals(0, version.patch());
  }

  @Test
  void shouldParseMajor() {
    Version version = Version.parseVersion("1");
    assertEquals("1.0.0", version.toString());
    assertEquals(1, version.major());
    assertEquals(0, version.minor());
    assertEquals(0, version.patch());
  }

  @Test
  void shouldParseMajorMinorPatchGarbage() {
    Version version = Version.parseVersion("1.2.3z-5.4");
    assertEquals("1.2.3", version.toString());
    assertEquals(1, version.major());
    assertEquals(2, version.minor());
    assertEquals(3, version.patch());
  }

  @Test
  void shouldParseMajorMinorGarbage() {
    Version version = Version.parseVersion("1.2z-5.4");
    assertEquals("1.2.0", version.toString());
    assertEquals(1, version.major());
    assertEquals(2, version.minor());
    assertEquals(0, version.patch());
  }

  @Test
  void shouldParseMajorGarbage() {
    Version version = Version.parseVersion("1z-5.4");
    assertEquals("1.0.0", version.toString());
    assertEquals(1, version.major());
    assertEquals(0, version.minor());
    assertEquals(0, version.patch());
  }

  @Test
  void shouldRejectNotStartingWithMajor() {
    assertThrows(IllegalArgumentException.class, () -> Version.parseVersion("a3.2.4"));
  }

  @Test
  void shouldRejectEmptyString() {
    assertThrows(IllegalArgumentException.class, () -> Version.parseVersion(""));
  }

  @Test
  void shouldRejectComponentTooLarge() {
    assertThrows(IllegalArgumentException.class, () -> Version.parseVersion(Long.toString(((long) Integer.MAX_VALUE) + 1)));
  }

  @Test
  void shouldNullPointerOnNullVersionString() {
    assertThrows(NullPointerException.class, () -> Version.parseVersion(null));
  }
}
