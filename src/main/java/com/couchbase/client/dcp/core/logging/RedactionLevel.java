/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import static java.util.Objects.requireNonNull;

/**
 * Allows to specify the level of log redaction.
 */
public enum RedactionLevel {
  /**
   * No redaction is performed, this is the default
   */
  NONE,
  /**
   * Only user data is redacted, system and metadata are not.
   */
  PARTIAL,
  /**
   * User, System and Metadata are all redacted.
   */
  FULL;

  private static volatile RedactionLevel level = NONE;

  public static RedactionLevel get() {
    return level;
  }

  public static void set(RedactionLevel level) {
    RedactionLevel.level = requireNonNull(level);
  }
}
