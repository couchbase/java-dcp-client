/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.highlevel;

public enum FlowControlMode {
  /**
   * In this mode, {@link Mutation} and {@link Deletion} events are automatically acknowledged
   * prior to dispatch to the {@link DatabaseChangeListener}.
   * <p>
   * Suitable for listeners whose {@link DatabaseChangeListener#onMutation(Mutation)}
   * and {@link DatabaseChangeListener#onDeletion(Deletion)} methods either completely process
   * the event (for example, by writing it to disk) or implictly generate backpressure (for example,
   * by writing the event to a bounded blocking queue).
   */
  AUTOMATIC,

  /**
   * In this mode, the listener is responsible for calling {@link DocumentChange#flowControlAck()}
   * after each {@link Mutation} and {@link Deletion} event is processed.
   * <p>
   * <B>NOTE:</B> This mode is not recommended, since it can be difficult to ensure events are
   * acknowledged on all code paths, and failure to ACK can cause the server to stop sending events.
   */
  MANUAL
}
