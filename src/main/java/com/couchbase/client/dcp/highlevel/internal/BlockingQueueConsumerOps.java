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

package com.couchbase.client.dcp.highlevel.internal;

import java.util.concurrent.TimeUnit;

/**
 * Subset of the BlockingQueue interface that should be exposed to consumers.
 *
 * @param <T> queue element type
 */
public interface BlockingQueueConsumerOps<T> {

  /**
   * Retrieves and removes the head of this queue, waiting if necessary
   * until an element becomes available.
   *
   * @return the head of this queue
   * @throws InterruptedException if interrupted while waiting
   */
  T take() throws InterruptedException;

  /**
   * Retrieves and removes the head of this queue, waiting up to the
   * specified wait time if necessary for an element to become available.
   *
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
   * @return the head of this queue, or {@code null} if the specified waiting time elapses
   * before an element is available
   * @throws InterruptedException if interrupted while waiting
   */
  T poll(long timeout, TimeUnit unit) throws InterruptedException;
}
