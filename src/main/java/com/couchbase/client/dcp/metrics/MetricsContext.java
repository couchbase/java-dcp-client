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

import io.micrometer.core.instrument.Tags;

public class MetricsContext {
  private final String prefix;
  private final Tags tags;

  public MetricsContext(String prefix) {
    this(prefix, Tags.empty());
  }
  public MetricsContext(String prefix, Tags tags) {
    this.prefix = prefix.isEmpty() || prefix.endsWith(".") ? prefix : prefix + ".";
    this.tags = tags;
  }

  public ActionCounter.Builder newActionCounter(String name) {
    return ActionCounter.builder(prefix + name)
        .tags(tags);
  }

  public EventCounter.Builder newEventCounter(String name) {
    return EventCounter.builder(prefix + name)
        .tags(tags);
  }

  public ActionTimer.Builder newActionTimer(String name) {
    return ActionTimer.builder(name)
        .tags(tags);
  }
}
