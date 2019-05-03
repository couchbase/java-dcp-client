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

package com.couchbase.client.dcp.util;

import com.couchbase.client.core.time.Delay;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * A stateful wrapper around a {@link Delay}. Manages the retry counter
 * and automatically resets it to zero after a cooldown duration.
 */
public class AdaptiveDelay {
  /**
   * Enables setting the "current time" from test code.
   * Simpler than {@link java.time.Clock} and doesn't require a mock.
   */
  interface TimeProvider {
    long nanoTime();
  }

  private final Delay delay;
  private final long cooldownNanos;
  private long attempt;
  private long lastEventNanos;
  private final TimeProvider clock;

  public AdaptiveDelay(Delay backoffStrategy, Duration cooldown) {
    this(backoffStrategy, cooldown, System::nanoTime);
  }

  AdaptiveDelay(Delay delay, Duration cooldown, TimeProvider clock) {
    this.delay = requireNonNull(delay);
    this.clock = requireNonNull(clock);
    this.cooldownNanos = cooldown.toNanos();
    this.lastEventNanos = clock.nanoTime() - cooldownNanos; // start cool
  }

  /**
   * Returns the next delay in the backoff sequence.
   * <p>
   * If the cooldown duration has elapsed since the previous delay returned by this method,
   * the backoff sequence is reset and this method returns a duration of zero.
   */
  public Duration calculate() {
    final long now = clock.nanoTime();

    synchronized (this) {
      final long nanosSinceLastEvent = now - lastEventNanos;
      final boolean cooldownElapsed = nanosSinceLastEvent >= cooldownNanos;

      if (cooldownElapsed) {
        attempt = 0;
        lastEventNanos = now;
        return Duration.ZERO;
      }

      final long delayNanos = delay.unit().toNanos(delay.calculate(++attempt));
      lastEventNanos = now + delayNanos;
      return Duration.ofNanos(delayNanos);
    }
  }
}
