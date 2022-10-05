/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.core.utils;

import reactor.util.annotation.Nullable;

public class CbStrings {
  private CbStrings() {
    throw new AssertionError("not instantiable");
  }

  public static String defaultIfEmpty(@Nullable String s, String defaultValue) {
    return s == null || s.isEmpty()
        ? defaultValue
        : s;
  }
}
