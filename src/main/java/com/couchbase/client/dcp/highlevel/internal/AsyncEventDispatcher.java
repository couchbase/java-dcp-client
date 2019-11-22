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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.FlowControlMode;
import com.couchbase.client.dcp.highlevel.StreamFailure;

import java.time.Duration;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;

import static java.util.Objects.requireNonNull;

/**
 * Dispatches events in a separate dedicated thread.
 */
public class AsyncEventDispatcher implements EventDispatcher {
  private static final CouchbaseLogger log = CouchbaseLoggerFactory.getInstance(AsyncEventDispatcher.class);

  private static class GracefulShutdownPoisonPill extends RuntimeException {
  }

  private final BlockingDeque<DatabaseChangeEvent> queue = new LinkedBlockingDeque<>();
  private final BlockingQueueConsumerOps<DatabaseChangeEvent> queueOps;

  private volatile boolean shutdown;

  private static final ThreadFactory threadFactory =
      new SimpleThreadFactory("dcp-event-dispatch-", t -> t.setDaemon(true));

  private final Thread thread;

  public AsyncEventDispatcher(FlowControlMode flowControlMode, DatabaseChangeListener consumer) {
    requireNonNull(consumer);
    this.queueOps = new DatabaseChangeConsumerOps(queue, flowControlMode);

    this.thread = threadFactory.newThread(() -> {
      while (!shutdown) {
        try {
          queueOps.take().dispatch(consumer);

        } catch (GracefulShutdownPoisonPill e) {
          log.info("High-level event dispatcher terminated due to graceful shutdown request.");
          return;

        } catch (InterruptedException e) {
          log.info("High-level event dispatcher terminated due to interruption.");
          return;

        } catch (Throwable t) {
          try {
            log.warn("Event listener threw exception.", t);
            consumer.onFailure(new StreamFailure(-1, t));

          } catch (Throwable anotherFineMess) {
            log.error("Event listener error handler threw exception.", anotherFineMess);
          }
        }
      }
    });

    thread.start();
  }

  @Override
  public void dispatch(DatabaseChangeEvent event) {
    queue.add(event);
  }

  /**
   * Interrupts the listener and starts terminating the dispatch thread.
   *
   * <b>NOTE:</b> Any events remaining in the queue will not be dispatched
   * and will not be flow control ACK'd. Consequently, this method should
   * only be called as part of the DCP channel shutdown process.
   */
  @Override
  public void shutdownNow() {
    shutdown = true;
    thread.interrupt();
  }

  /**
   * Allows the listener to finish processing the current event and then
   * starts terminating the dispatch thread.
   *
   * <b>NOTE:</b> Any events remaining in the queue will not be dispatched
   * and will not be flow control ACK'd. Consequently, this method should
   * only be called as part of the DCP channel shutdown process.
   */
  @Override
  public void gracefulShutdown() {
    queue.addFirst(poisonPill());
  }

  /**
   * Blocks until the dispatch thread terminates or the timeout expires.
   *
   * @return true if the thread terminated within the timeout period
   */
  @Override
  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    thread.join(timeout.toMillis());
    return !thread.isAlive();
  }

  private static DatabaseChangeEvent poisonPill() {
    return new DatabaseChangeEvent() {
      @Override
      public void dispatch(DatabaseChangeListener listener) {
        throw new GracefulShutdownPoisonPill();
      }

      @Override
      public int getVbucket() {
        return -1;
      }
    };
  }
}
