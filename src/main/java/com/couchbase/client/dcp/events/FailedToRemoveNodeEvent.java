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

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.utils.Events;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Event published when the connector has failed to remove node during failover/rebalance.
 */
public class FailedToRemoveNodeEvent implements CouchbaseEvent {
  private final InetSocketAddress node;
  private final Throwable error;

  public FailedToRemoveNodeEvent(InetSocketAddress node, Throwable error) {
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
  public InetSocketAddress node() {
    return node;
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
    return "FailedToRemoveNodeEvent{" +
        "node=" + node +
        "error=" + error +
        '}';
  }
}
