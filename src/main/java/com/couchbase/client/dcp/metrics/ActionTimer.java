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

import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
    private LogLevel successLogLevel = LogLevel.INFO;
    private LogLevel failureLogLevel = LogLevel.WARN;

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

    public Builder logLevel(LogLevel logLevel) {
      return successLogLevel(logLevel)
          .failureLogLevel(logLevel);
    }

    public Builder successLogLevel(LogLevel logLevel) {
      this.successLogLevel = requireNonNull(logLevel);
      return this;
    }

    public Builder failureLogLevel(LogLevel logLevel) {
      this.failureLogLevel = requireNonNull(logLevel);
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
  private final LogLevel successLogLevel;
  private final LogLevel failureLogLevel;
  private final Logger logger;

  public static Builder builder(String name) {
    return new Builder(name);
  }

  private ActionTimer(MeterRegistry registry, String name, Iterable<Tag> tags, Clock clock,
                      LogLevel successLogLevel, LogLevel failureLogLevel) {
    this.registry = requireNonNull(registry);
    this.name = requireNonNull(name);
    this.clock = requireNonNull(clock);
    this.successLogLevel = requireNonNull(successLogLevel);
    this.failureLogLevel = requireNonNull(failureLogLevel);
    this.logger = LoggerFactory.getLogger(ActionTimer.class.getName() + "." + name);

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
    successLogLevel.log(logger, "success {}", baseTags);
    successTimer.record(elapsed, unit);
  }

  /**
   * Increments the "failure" count.
   *
   * @param reason (nullable) cause of the failure, or null if unknown
   */
  public void failure(long elapsed, TimeUnit unit, Throwable reason) {
    failureLogLevel.log(logger, "failure {}", baseTags, reason);
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
