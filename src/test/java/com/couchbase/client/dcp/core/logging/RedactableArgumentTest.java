/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.core.logging;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedactableArgumentTest {
  private static RedactionLevel origLevel;

  @BeforeAll
  public static void saveOrigLevel() {
    origLevel = RedactionLevel.get();
  }

  @AfterAll
  public static void restoreOrigLevel() {
    RedactionLevel.set(origLevel);
  }

  @Test
  void shouldNotRedactLogsWhenDisabled() {
    RedactionLevel.set(RedactionLevel.NONE);

    assertEquals("1", RedactableArgument.user(1).toString());
    assertEquals("null", RedactableArgument.meta(null).toString());
    assertEquals("system", RedactableArgument.system("system").toString());
  }

  @Test
  void shouldOnlyRedactUserOnPartial() {
    RedactionLevel.set(RedactionLevel.PARTIAL);

    assertEquals("<ud>user</ud>", RedactableArgument.user("user").toString());
    assertEquals("meta", RedactableArgument.meta("meta").toString());
    assertEquals("system", RedactableArgument.system("system").toString());
  }

  @Test
  void forNowShouldRedactOnlyUserOnFull() {
    RedactionLevel.set(RedactionLevel.FULL);

    assertEquals("<ud>user</ud>", RedactableArgument.user("user").toString());
    assertEquals("meta", RedactableArgument.meta("meta").toString());
    assertEquals("system", RedactableArgument.system("system").toString());
  }
}
