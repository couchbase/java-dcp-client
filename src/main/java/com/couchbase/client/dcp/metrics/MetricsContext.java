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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

import static java.util.Objects.requireNonNull;

public class MetricsContext {
  private final MeterRegistry registry;
  private final String prefix;
  private final Tags tags;

  public MetricsContext(MeterRegistry registry, String prefix) {
    this(registry, prefix, Tags.empty());
  }

  public MetricsContext(MeterRegistry registry, String prefix, Tags tags) {
    this.registry = requireNonNull(registry);
    this.prefix = prefix.isEmpty() || prefix.endsWith(".") ? prefix : prefix + ".";
    this.tags = requireNonNull(tags);
  }

  public MeterRegistry registry() {
    return registry;
  }

  public MetricsContext withTags(Tags tags) {
    return new MetricsContext(registry, prefix, Tags.concat(this.tags, tags));
  }

  public ActionCounter.Builder newActionCounter(String name) {
    return ActionCounter.builder(registry, prefix + name)
        .tags(tags);
  }

  public EventCounter.Builder newEventCounter(String name) {
    return EventCounter.builder(registry, prefix + name)
        .tags(tags);
  }

  public ActionTimer.Builder newActionTimer(String name) {
    return ActionTimer.builder(registry, name)
        .tags(tags);
  }
}
