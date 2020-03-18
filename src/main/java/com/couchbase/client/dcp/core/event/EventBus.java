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
package com.couchbase.client.dcp.core.event;

import rx.Observable;

public interface EventBus {

  /**
   * Subscribe to the event bus to retrieve {@link CouchbaseEvent}s.
   *
   * @return the observable where the events are emitted into.
   */
  Observable<CouchbaseEvent> get();

  /**
   * Publish a {@link CouchbaseEvent} into the bus.
   *
   * @param event the event to publish.
   */
  void publish(CouchbaseEvent event);

  /**
   * Checks if the event bus has subscribers.
   *
   * This method can be utilized on the publisher side to avoid complex event creation when there is no one
   * on the other side listening and the event would be discarded immediately afterwards.
   *
   * @return true if it has subscribers, false otherwise.
   */
  boolean hasSubscribers();

}
