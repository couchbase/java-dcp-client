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

import com.couchbase.client.dcp.highlevel.FlowControlMode;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Optionally auto-acknowledges events as they are consumed.
 */
public class DatabaseChangeConsumerOps implements BlockingQueueConsumerOps<DatabaseChangeEvent> {
  private final BlockingQueue<DatabaseChangeEvent> queue;

  private final boolean ackOnConsume;

  public DatabaseChangeConsumerOps(BlockingQueue<DatabaseChangeEvent> queue, FlowControlMode flowControlMode) {
    this.queue = requireNonNull(queue);
    this.ackOnConsume = requireNonNull(flowControlMode) == FlowControlMode.AUTOMATIC;
  }

  @Override
  public DatabaseChangeEvent take() throws InterruptedException {
    return maybeAck(queue.take());
  }

  @Override
  public DatabaseChangeEvent poll(long timeout, TimeUnit unit) throws InterruptedException {
    return maybeAck(queue.poll(timeout, unit));
  }

  private DatabaseChangeEvent maybeAck(DatabaseChangeEvent change) {
    if (ackOnConsume && change instanceof FlowControllable) {
      ((FlowControllable) change).flowControlAck();
    }
    return change;
  }
}
