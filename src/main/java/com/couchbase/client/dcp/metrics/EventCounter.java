/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.metrics;

import com.couchbase.client.core.logging.CouchbaseLogLevel;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.couchbase.client.dcp.metrics.DcpMetricsHelper.log;
import static java.util.Objects.requireNonNull;

/**
 * Tracks occurrences of an event.
 * <p>
 * Instances are created via {@link #builder}.
 */
public class EventCounter {
  public static class Builder {
    private final String name;
    private MeterRegistry registry = Metrics.globalRegistry;
    private List<Tag> baseTags = new ArrayList<>();
    private CouchbaseLogLevel logLevel = CouchbaseLogLevel.INFO;

    private Builder(String name) {
      this.name = requireNonNull(name);
    }

    public Builder registry(MeterRegistry registry) {
      this.registry = requireNonNull(registry);
      return this;
    }

    public Builder tag(String key, String value) {
      return tag(Tag.of(key, value));
    }

    public Builder tag(Tag tag) {
      this.baseTags.add(requireNonNull(tag));
      return this;
    }

    public Builder tags(Iterable<Tag> tag) {
      tag.forEach(this::tag);
      return this;
    }

    /**
     * @param logLevel (nullable) null means do not log
     */
    public Builder logLevel(CouchbaseLogLevel logLevel) {
      this.logLevel = logLevel;
      return this;
    }

    public EventCounter build() {
      return new EventCounter(registry, name, baseTags, logLevel);
    }
  }

  private final String name;
  private final Counter counter;
  private final MeterRegistry registry;
  private final List<Tag> baseTags;
  private final CouchbaseLogLevel logLevel;
  private final CouchbaseLogger logger;

  public static Builder builder(String name) {
    return new Builder(name);
  }

  private EventCounter(MeterRegistry registry, String name, Iterable<Tag> tags,
                       CouchbaseLogLevel logLevel) {
    this.registry = requireNonNull(registry);
    this.name = requireNonNull(name);
    this.logLevel = logLevel;
    this.logger = CouchbaseLoggerFactory.getInstance(EventCounter.class.getName() + "." + name);

    List<Tag> tagList = new ArrayList<>();
    tags.forEach(tagList::add);
    this.baseTags = Collections.unmodifiableList(tagList);

    this.counter = registry.counter(name, baseTags);
  }

  /**
   * Increments the event count.
   */
  public void increment() {
    log(logger, logLevel, "event {}", baseTags);
    counter.increment();
  }

  /**
   * Increases the event count by the given amount
   */
  public void increment(long amount) {
    if (logger != null) { // null check to skip boxing of amount when logging is disabled
      log(logger, logLevel, "event {} {}", amount, baseTags);
    }
    counter.increment(amount);
  }
}
