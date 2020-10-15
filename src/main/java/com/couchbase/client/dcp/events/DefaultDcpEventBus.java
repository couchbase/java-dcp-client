/*
 * Copyright 2017 Couchbase, Inc.
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

package com.couchbase.client.dcp.events;

import com.couchbase.client.dcp.core.event.CouchbaseEvent;
import com.couchbase.client.dcp.core.event.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

/**
 * Like {@link com.couchbase.client.dcp.core.event.DefaultEventBus} but buffers on backpressure instead of dropping.
 * This is important in order not to drop critical events like {@link StreamEndEvent}s that only get sent once.
 *
 * @since 0.13.0
 */
public class DefaultDcpEventBus implements EventBus {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDcpEventBus.class);

  private final FluxProcessor<CouchbaseEvent, CouchbaseEvent> bus = DirectProcessor.create();
  private final FluxSink<CouchbaseEvent> sink = bus.sink(FluxSink.OverflowStrategy.BUFFER);

  private final Scheduler scheduler;

  public DefaultDcpEventBus(final Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public Flux<CouchbaseEvent> get() {
    return bus.onBackpressureBuffer().publishOn(scheduler);
  }

  @Override
  public void publish(final CouchbaseEvent event) {
    try {
      sink.next(event);
    } catch (Exception ex) {
      LOGGER.warn("Caught exception during event emission, moving on.", ex);
    }
  }
}
