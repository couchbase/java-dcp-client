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

import rx.Observable;

/**
 * A stateful component that changes its state and notifies subscribed parties.
 */
public interface Stateful<S extends Enum> {

  /**
   * Returns a infinite observable which gets updated when the state of the component changes.
   *
   * @return a {@link Observable} updated with state transitions.
   */
  Observable<S> states();

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

  /**
   * Returns true if there are subscribers observing the state stream.
   *
   * @return true if at least one does, false otherwise.
   */
  boolean hasSubscribers();
}
