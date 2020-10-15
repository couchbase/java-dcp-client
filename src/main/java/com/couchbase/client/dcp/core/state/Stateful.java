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
package com.couchbase.client.dcp.core.state;

/**
 * A stateful component that changes its state and notifies subscribed parties.
 */
public interface Stateful<S extends Enum> {


  /**
   * Returns the current state.
   *
   * @return the current state.
   */
  S state();

  /**
   * Check if the given state is the same as the current one.
   *
   * @param state the stats to check against.
   * @return true if it is the same, false otherwise.
   */
  boolean isState(S state);

}
