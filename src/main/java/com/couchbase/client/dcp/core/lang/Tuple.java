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
package com.couchbase.client.dcp.core.lang;

/**
 * Static factory class for various Tuples.
 * <p>
 * A tuple should be used if more than one argument needs to be passed through a observable. Note that there are
 * intentionally only tuples with a maximum of five elements, because if more are needed it hurts readability and
 * value objects should be used instead. Also keep in mind that if a tuple is used instead of a value object semantics
 * might get lost, so use them sparingly.
 */
public final class Tuple {

  /**
   * Forbidding instantiation.
   */
  private Tuple() {
  }

  /**
   * Creates a tuple with two values.
   *
   * @param v1 the first value.
   * @param v2 the second value.
   * @return a tuple containing the values.
   */
  public static <T1, T2> Tuple2<T1, T2> create(final T1 v1, final T2 v2) {
    return new Tuple2<T1, T2>(v1, v2);
  }

}
