/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.dcp.config;

import static java.util.Objects.requireNonNull;

public class Control {
  private final String name;
  private final String value;
  private final boolean optional;

  public Control(String name, String value, boolean optional) {
    this.name = requireNonNull(name);
    this.value = requireNonNull(value);
    this.optional = optional;
  }

  public String name() {
    return name;
  }

  public String value() {
    return value;
  }

  public boolean isOptional() {
    return optional;
  }

  @Override
  public String toString() {
    return name + "=" + value + (optional ? " (optional)" : "");
  }
}
