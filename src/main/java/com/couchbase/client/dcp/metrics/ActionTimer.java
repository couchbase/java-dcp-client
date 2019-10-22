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
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.dcp.metrics.DcpMetricsHelper.log;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Tracks the results and of an action that can succeed or fail, along with elapsed time.
 * <p>
 * Instances are created via {@link #builder}.
 * <p>
 * The tags match those used by {@link io.micrometer.core.aop.CountedAspect}.
 */
public class ActionTimer {
  public static class Builder {
    private final String name;
    private Clock clock = Clock.SYSTEM;
    private MeterRegistry registry = Metrics.globalRegistry;
    private List<Tag> baseTags = new ArrayList<>();
    private CouchbaseLogLevel successLogLevel = CouchbaseLogLevel.INFO;
    private CouchbaseLogLevel failureLogLevel = CouchbaseLogLevel.WARN;

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
      return successLogLevel(logLevel)
          .failureLogLevel(logLevel);
    }

    /**
     * @param logLevel (nullable) null means do not log
     */
    public Builder successLogLevel(CouchbaseLogLevel logLevel) {
      this.successLogLevel = logLevel;
      return this;
    }

    /**
     * @param logLevel (nullable) null means do not log
     */
    public Builder failureLogLevel(CouchbaseLogLevel logLevel) {
      this.failureLogLevel = logLevel;
      return this;
    }

    public Builder clock(Clock clock) {
      this.clock = requireNonNull(clock);
      return this;
    }

    public ActionTimer build() {
      return new ActionTimer(registry, name, baseTags, clock, successLogLevel, failureLogLevel);
    }
  }

  private final String name;
  private final Clock clock;
  private final Timer successTimer;
  private final MeterRegistry registry;
  private final List<Tag> baseTags;
  private final CouchbaseLogLevel successLogLevel;
  private final CouchbaseLogLevel failureLogLevel;
  private final CouchbaseLogger logger;

  public static Builder builder(String name) {
    return new Builder(name);
  }

  private ActionTimer(MeterRegistry registry, String name, Iterable<Tag> tags, Clock clock,
                      CouchbaseLogLevel successLogLevel, CouchbaseLogLevel failureLogLevel) {
    this.registry = requireNonNull(registry);
    this.name = requireNonNull(name);
    this.clock = requireNonNull(clock);
    this.successLogLevel = successLogLevel;
    this.failureLogLevel = failureLogLevel;
    this.logger = CouchbaseLoggerFactory.getInstance(ActionTimer.class.getName() + "." + name);

    List<Tag> tagList = new ArrayList<>();
    tags.forEach(tagList::add);
    this.baseTags = Collections.unmodifiableList(tagList);

    List<Tag> successTags = new ArrayList<>(baseTags);
    successTags.add(Tag.of("result", "success"));
    successTags.add(Tag.of("exception", "none"));
    this.successTimer = registry.timer(name, successTags);
  }

  public void track(Future<?> f) {
    final long start = clock.monotonicTime();

    f.addListener(future -> {
      final long elapsed = clock.monotonicTime() - start;
      if (f.isSuccess()) {
        success(elapsed, NANOSECONDS);
      } else {
        failure(elapsed, NANOSECONDS, f.cause());
      }
    });
  }

  /**
   * Increments the "success" count.
   */
  public void success(long elapsed, TimeUnit unit) {
    log(logger, successLogLevel, "success {}", baseTags);
    successTimer.record(elapsed, unit);
  }

  /**
   * Increments the "failure" count.
   *
   * @param reason (nullable) cause of the failure, or null if unknown
   */
  public void failure(long elapsed, TimeUnit unit, Throwable reason) {
    // Don't want to convert reason to string here but it's necessary otherwise the placeholder doesn't get replaced.
    // Might be a bug in how Throwable parameters are handled by CouchbaseLogger.
    // todo: remove the string conversion, after migrating from CouchbaseLogger to pure slf4j
    log(logger, failureLogLevel, "failure {} : {}", baseTags, String.valueOf(reason));
    String reasonName = reason == null ? "unknown" : reason.getClass().getSimpleName();
    failure(elapsed, unit, reasonName);
  }

  public void failure(long elapsed, TimeUnit unit, String exception) {
    Timer.builder(name)
        .tags(baseTags)
        .tag("result", "failure")
        .tag("exception", exception)
        .register(registry)
        .record(elapsed, unit);
  }
}
