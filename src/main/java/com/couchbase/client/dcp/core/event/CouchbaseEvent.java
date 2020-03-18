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

import java.util.Map;

/**
 * Defines the generic couchbase event pushed through the event bus.
 */
public interface CouchbaseEvent {

  /**
   * The type of the event.
   *
   * @return the event type.
   */
  EventType type();

  /**
   * Converts the event into a map representation of simple types.
   *
   * @return the map representation.
   */
  Map<String, Object> toMap();

}
