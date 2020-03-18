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
 * Delay which increases linearly for every attempt.
 */
public class LinearDelay extends Delay {

  private final double growBy;
  private final long lower;
  private final long upper;

  LinearDelay(TimeUnit unit, long upper, long lower, double growBy) {
    super(unit);
    if (lower > upper) {
      throw new IllegalArgumentException("The lower value must be smaller or equal to the upper value!");
    }
    this.growBy = growBy;
    this.lower = lower;
    this.upper = upper;
  }

  @Override
  public long calculate(long attempt) {
    long calc = Math.round(attempt * growBy);
    if (calc < lower) {
      return lower;
    }
    if (calc > upper) {
      return upper;
    }
    return calc;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("LinearDelay{");
    sb.append("growBy ").append(growBy);
    sb.append(" " + unit());
    sb.append("; lower=").append(lower);
    sb.append(", upper=").append(upper);
    sb.append('}');
    return sb.toString();
  }
}
