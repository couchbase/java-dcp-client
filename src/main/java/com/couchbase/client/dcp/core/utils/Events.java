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
package com.couchbase.client.dcp.core.utils;

import com.couchbase.client.dcp.core.CouchbaseException;
import com.couchbase.client.dcp.core.event.CouchbaseEvent;


import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for event handling.
 */
public class Events {

  /**
   * Takes a {@link CouchbaseEvent} and returns a map with event information.
   *
   * @param source the source event.
   * @return a new map which contains name and type info in an event sub-map.
   */
  public static Map<String, Object> identityMap(CouchbaseEvent source) {
    Map<String, Object> root = new HashMap<String, Object>();
    Map<String, String> event = new HashMap<String, String>();

    event.put("name", source.getClass().getSimpleName().replaceAll("Event$", ""));
    event.put("type", source.type().toString());
    root.put("event", event);

    return root;
  }
}
