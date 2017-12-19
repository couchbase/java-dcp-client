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

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;

/**
 * Like {@link com.couchbase.client.core.event.DefaultEventBus} but buffers on backpressure instead of dropping.
 * This is important in order not to drop critical events like {@link StreamEndEvent}s that only get sent once.
 *
 * @since 0.13.0
 */
public class DefaultDcpEventBus implements EventBus {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DefaultDcpEventBus.class);

    private final SerializedSubject<CouchbaseEvent, CouchbaseEvent> bus = PublishSubject.<CouchbaseEvent>create().toSerialized();
    private final Scheduler scheduler;

    public DefaultDcpEventBus(final Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Observable<CouchbaseEvent> get() {
        return bus.onBackpressureBuffer().observeOn(scheduler);
    }

    @Override
    public void publish(final CouchbaseEvent event) {
        if (bus.hasObservers()) {
            try {
                bus.onNext(event);
            } catch (Exception ex) {
                LOGGER.warn("Caught exception during event emission, moving on.", ex);
            }
        }
    }

    @Override
    public boolean hasSubscribers() {
        return bus.hasObservers();
    }
}
