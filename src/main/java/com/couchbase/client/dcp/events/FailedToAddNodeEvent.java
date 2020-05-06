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

package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.event.CouchbaseEvent;
import com.couchbase.client.dcp.core.event.EventType;
import com.couchbase.client.dcp.core.utils.Events;

import java.util.Map;
import java.util.OptionalInt;

/**
 * Event published when the connector has failed to add new node during failover/rebalance.
 */
public class FailedToAddNodeEvent implements CouchbaseEvent, DcpFailureEvent {
  private final HostAndPort node;
  private final Throwable error;

  public FailedToAddNodeEvent(HostAndPort node, Throwable error) {
    this.node = node;
    this.error = error;
  }

  @Override
  public EventType type() {
    return EventType.SYSTEM;
  }

  /**
   * The address of the node
   */
  public HostAndPort node() {
    return node;
  }

  @Override
  public OptionalInt partition() {
    return OptionalInt.empty();
  }

  /**
   * Error object, describing the issue
   */
  public Throwable error() {
    return error;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> result = Events.identityMap(this);
    result.put("node", node);
    result.put("error", error);
    return result;
  }

  @Override
  public String toString() {
    return "FailedToAddNodeEvent{" +
        "node=" + node +
        "error=" + error +
        '}';
  }
}
