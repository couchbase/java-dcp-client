/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.core.utils;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.EventSubscription;
import com.couchbase.client.core.cnc.LoggingEventConsumer;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * Simple implementation that synchronously logs events.
 */
@Stability.Internal
public class MinimalEventBus implements EventBus {

  public static final EventBus INSTANCE = new MinimalEventBus();

  private final LoggingEventConsumer loggingEventConsumer = LoggingEventConsumer.create();

  @Override
  public synchronized PublishResult publish(Event event) {
    loggingEventConsumer.accept(event);
    return PublishResult.SUCCESS;
  }

  @Override
  public EventSubscription subscribe(Consumer<Event> consumer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unsubscribe(EventSubscription subscription) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Mono<Void> start() {
    return Mono.empty();
  }

  @Override
  public Mono<Void> stop(Duration timeout) {
    return Mono.empty();
  }
}
