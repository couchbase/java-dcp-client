/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.core.time;

import java.util.concurrent.TimeUnit;

/**
 * Parent class of {@link Delay}s and provides factory methods to create them.
 */
public abstract class Delay {

  /**
   * The time unit of the delay.
   */
  private final TimeUnit unit;

  /**
   * Creates a new {@link Delay}.
   *
   * @param unit the time unit.
   */
  Delay(TimeUnit unit) {
    if (unit == null) {
      throw new IllegalArgumentException("TimeUnit is not allowed to be null");
    }

    this.unit = unit;
  }

  /**
   * Returns the {@link TimeUnit} associated with this {@link Delay}.
   *
   * @return the time unit.
   */
  public TimeUnit unit() {
    return unit;
  }

  /**
   * Calculate a specific delay based on the attempt passed in.
   * <p>
   * This method is to be implemented by the child implementations and depending on the params
   * that were set during construction time.
   *
   * @param attempt the attempt to calculate the delay from.
   * @return the calculate delay.
   */
  public abstract long calculate(long attempt);

  /**
   * Creates a new {@link FixedDelay}.
   *
   * @param delay the time of the delay.
   * @param unit the unit of the delay.
   * @return a created {@link FixedDelay}.
   */
  public static Delay fixed(long delay, TimeUnit unit) {
    return new FixedDelay(delay, unit);
  }

  /**
   * Creates a new {@link LinearDelay} with no bounds and default factor.
   *
   * @param unit the unit of the delay.
   * @return a created {@link LinearDelay}.
   */
  public static Delay linear(TimeUnit unit) {
    return linear(unit, Long.MAX_VALUE);
  }

  /**
   * Creates a new {@link LinearDelay} with a custom upper boundary and the default factor.
   *
   * @param unit the unit of the delay.
   * @param upper the upper boundary.
   * @return a created {@link LinearDelay}.
   */
  public static Delay linear(TimeUnit unit, long upper) {
    return linear(unit, upper, 0);
  }

  /**
   * Creates a new {@link LinearDelay} with a custom boundaries and the default factor.
   *
   * @param unit the unit of the delay.
   * @param upper the upper boundary.
   * @param lower the lower boundary.
   * @return a created {@link LinearDelay}.
   */
  public static Delay linear(TimeUnit unit, long upper, long lower) {
    return linear(unit, upper, lower, 1);
  }

  /**
   * Creates a new {@link LinearDelay} with a custom boundaries and factor.
   *
   * @param unit the unit of the delay.
   * @param upper the upper boundary.
   * @param lower the lower boundary.
   * @param growBy the multiplication factor.
   * @return a created {@link LinearDelay}.
   */
  public static Delay linear(TimeUnit unit, long upper, long lower, long growBy) {
    return new LinearDelay(unit, upper, lower, growBy);
  }

  /**
   * Creates a new {@link ExponentialDelay} with default boundaries and factor (1, 2, 4, 8, 16, 32...).
   *
   * @param unit the unit of the delay.
   * @return a created {@link ExponentialDelay}.
   */
  public static Delay exponential(TimeUnit unit) {
    return exponential(unit, Long.MAX_VALUE);
  }

  /**
   * Creates a new {@link ExponentialDelay} with custom upper boundary and default factor (eg. with upper 8: 1, 2, 4,
   * 8, 8, 8...).
   *
   * @param unit the unit of the delay.
   * @param upper the upper boundary.
   * @return a created {@link ExponentialDelay}.
   */
  public static Delay exponential(TimeUnit unit, long upper) {
    return exponential(unit, upper, 0);
  }

  /**
   * Creates a new {@link ExponentialDelay} with custom boundaries and default factor (eg. with upper 8, lower 3: 3,
   * 3, 4, 8, 8, 8...).
   *
   * @param unit the unit of the delay.
   * @param upper the upper boundary.
   * @param lower the lower boundary.
   * @return a created {@link ExponentialDelay}.
   */
  public static Delay exponential(TimeUnit unit, long upper, long lower) {
    return exponential(unit, upper, lower, 1);
  }

  /**
   * Creates a new {@link ExponentialDelay} with custom boundaries and factor (eg. with upper 300, lower 0, growBy 10:
   * 10, 20, 40, 80, 160, 300, ...).
   *
   * @param unit the unit of the delay.
   * @param upper the upper boundary.
   * @param lower the lower boundary.
   * @param growBy the multiplication factor.
   * @return a created {@link ExponentialDelay}.
   */
  public static Delay exponential(TimeUnit unit, long upper, long lower, long growBy) {
    return exponential(unit, upper, lower, growBy, 2);
  }

  /**
   * Creates a new {@link ExponentialDelay} on a base different from powers of two, with custom boundaries and factor
   * (eg. with upper 9000, lower 0, growBy 3, powerOf 10: 3, 30, 300, 3000, 9000, 9000, 9000, ...).
   *
   * @param unit the unit of the delay.
   * @param upper the upper boundary.
   * @param lower the lower boundary.
   * @param growBy the multiplication factor (or basis for the size of each delay).
   * @param powersOf the base for exponential growth (eg. powers of 2, powers of 10, etc...)
   * @return a created {@link ExponentialDelay}.
   */
  public static Delay exponential(TimeUnit unit, long upper, long lower, long growBy, int powersOf) {
    return new ExponentialDelay(unit, upper, lower, growBy, powersOf);
  }

}
