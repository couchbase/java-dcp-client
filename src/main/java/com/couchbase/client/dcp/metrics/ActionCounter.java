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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.couchbase.client.dcp.metrics.DcpMetricsHelper.log;
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

    public ActionCounter build() {
      return new ActionCounter(registry, name, baseTags, successLogLevel, failureLogLevel);
    }
  }

  private final String name;
  private final Counter successCounter;
  private final MeterRegistry registry;
  private final List<Tag> baseTags;
  private final CouchbaseLogLevel successLogLevel;
  private final CouchbaseLogLevel failureLogLevel;
  private final CouchbaseLogger logger;

  public static Builder builder(String name) {
    return new Builder(name);
  }

  private ActionCounter(MeterRegistry registry, String name, Iterable<Tag> tags,
                        CouchbaseLogLevel successLogLevel, CouchbaseLogLevel failureLogLevel) {
    this.registry = requireNonNull(registry);
    this.name = requireNonNull(name);
    this.successLogLevel = successLogLevel;
    this.failureLogLevel = failureLogLevel;
    this.logger = CouchbaseLoggerFactory.getInstance(ActionCounter.class.getName() + "." + name);

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
    log(logger, successLogLevel, "success {}", baseTags);
    successCounter.increment();
  }

  /**
   * Increments the "failure" count.
   *
   * @param reason (nullable) cause of the failure, or null if unknown
   */
  public void failure(Throwable reason) {
    // Don't want to convert reason to string here but it's necessary otherwise the placeholder doesn't get replaced.
    // Might be a bug in how Throwable parameters are handled by CouchbaseLogger.
    // todo: remove the string conversion, after migrating from CouchbaseLogger to pure slf4j
    log(logger, failureLogLevel, "failure {} : {}", baseTags, String.valueOf(reason));
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
