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

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Tracks the results of an action that can succeed or fail.
 * <p>
 * Instances are created via {@link #builder}.
 * <p>
 * The tags match those used by {@link io.micrometer.core.aop.CountedAspect}.
 */
public class ActionCounter {
  public static class Builder {
    private final String name;
    private final MeterRegistry registry;
    private List<Tag> baseTags = new ArrayList<>();
    private LogLevel successLogLevel = LogLevel.INFO;
    private LogLevel failureLogLevel = LogLevel.WARN;

    private Builder(MeterRegistry registry, String name) {
      this.registry = requireNonNull(registry);
      this.name = requireNonNull(name);
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

    public ActionCounter build() {
      return new ActionCounter(registry, name, baseTags, successLogLevel, failureLogLevel);
    }
  }

  private final String name;
  private final Counter successCounter;
  private final MeterRegistry registry;
  private final List<Tag> baseTags;
  private final LogLevel successLogLevel;
  private final LogLevel failureLogLevel;
  private final Logger logger;

  public static Builder builder(MeterRegistry registry, String name) {
    return new Builder(registry, name);
  }

  private ActionCounter(MeterRegistry registry, String name, Iterable<Tag> tags,
                        LogLevel successLogLevel, LogLevel failureLogLevel) {
    this.registry = requireNonNull(registry);
    this.name = requireNonNull(name);
    this.successLogLevel = requireNonNull(successLogLevel);
    this.failureLogLevel = requireNonNull(failureLogLevel);
    this.logger = LoggerFactory.getLogger(ActionCounter.class.getName() + "." + name);

    List<Tag> tagList = new ArrayList<>();
    tags.forEach(tagList::add);
    this.baseTags = Collections.unmodifiableList(tagList);

    List<Tag> successTags = new ArrayList<>(baseTags);
    successTags.add(Tag.of("result", "success"));
    successTags.add(Tag.of("exception", "none"));
    this.successCounter = registry.counter(name, successTags);
  }

  public <V, F extends Future<V>> F track(F future) {
    future.addListener((GenericFutureListener<F>) f -> {
      if (f.isSuccess()) {
        success();
      } else {
        failure(f.cause());
      }
    });
    return future;
  }

  public <V, F extends Future<V>> F track(F future, Function<V, String> errorExtractor) {
    future.addListener((GenericFutureListener<F>) f -> {
      if (!f.isSuccess()) {
        failure(f.cause());
      } else {
        String error = errorExtractor.apply(f.getNow());
        if (error != null) {
          failure(error);
        } else {
          success();
        }
      }
    });
    return future;
  }

  /**
   * Increments the "success" count.
   */
  public void success() {
    successLogLevel.log(logger, "success {}", baseTags);
    successCounter.increment();
  }

  /**
   * Increments the "failure" count.
   *
   * @param reason (nullable) cause of the failure, or null if unknown
   */
  public void failure(Throwable reason) {
    failureLogLevel.log(logger, "failure {}", baseTags, reason);
    String reasonName = reason == null ? "unknown" : reason.getClass().getSimpleName();
    failure(reasonName);
  }

  public void failure(String exception) {
    Counter.builder(name)
        .tags(baseTags)
        .tag("result", "failure")
        .tag("exception", exception)
        .register(registry)
        .increment();
  }

  public void run(Runnable task) {
    try {
      task.run();
      success();
    } catch (Throwable t) {
      failure(t);
      throw t;
    }
  }

  public <T> T call(Callable<T> task) throws Exception {
    try {
      T result = task.call();
      success();
      return result;
    } catch (Throwable t) {
      failure(t);
      throw t;
    }
  }

  public Runnable wrap(Runnable r) {
    return () -> run(r);
  }

  public <T> Callable<T> wrap(Callable<T> c) {
    return () -> call(c);
  }
}
