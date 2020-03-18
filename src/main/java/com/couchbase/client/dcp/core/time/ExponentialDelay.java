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
 * Delay which increases exponentially on every attempt.
 *
 * Considering retry attempts start at 1, attempt 0 would be the initial call and will always yield 0 (or the lower bound).
 * Then each retry step will by default yield <code>1 * 2 ^ (attemptNumber-1)</code>. Actually each step can be based on a
 * different number than 1 unit of time using the <code>growBy</code> parameter: <code>growBy * 2 ^ (attemptNumber-1)</code>.
 *
 * By default with growBy = 1 this gives us 0 (initial attempt), 1, 2, 4, 8, 16, 32...
 *
 * Each of the resulting values that is below the <code>lowerBound</code> will be replaced by the lower bound, and
 * each value over the <code>upperBound</code> will be replaced by the upper bound.
 *
 * For example, given the following <code>Delay.exponential(TimeUnit.MILLISECONDS, 4000, 0, 500)</code>
 *
 *  * the upper of 4000 means the delay will be capped at 4s
 *  * the lower of 0 is useful to allow for immediate execution of original attempt, attempt 0 (if we ever call the
 *  delay with a parameter of 0)
 *  * the growBy of 500 means that we take steps based on 500ms
 *
 * This yields the following delays: <code>0ms, 500ms, 1s, 2s, 4s, 4s, 4s,...</code>
 *
 * In detail : <code>0, 500 * 2^0, 500 * 2^1, 500 * 2^2, 500 * 2^3, max(4000, 500 * 2^4), max(4000, 500 * 2^5),...</code>.
 *
 * Finally, the powers used in the computation can be changed from powers of two by default to another base using the
 * powersOf parameter.
 */
public class ExponentialDelay extends Delay {

  private final long lower;
  private final long upper;
  private final double growBy;
  private final int powersOf;

  ExponentialDelay(TimeUnit unit, long upper, long lower, double growBy, int powersOf) {
    super(unit);
    if (lower > upper) {
      throw new IllegalArgumentException("The lower value must be smaller or equal to the upper value!");
    }
    this.lower = lower;
    this.upper = upper;
    this.growBy = growBy;
    this.powersOf = powersOf <= 2 ? 2 : powersOf;
  }

  @Override
  public long calculate(long attempt) {
    long calc;
    if (attempt <= 0) { //safeguard against underflow
      calc = 0;
    } else if (powersOf == 2) {
      calc = calculatePowerOfTwo(attempt);
    } else {
      calc = calculateAlternatePower(attempt);
    }

    return applyBounds(calc);
  }

  protected long calculateAlternatePower(long attempt) {
    //round will cap at Long.MAX_VALUE and pow should prevent overflows
    double step = Math.pow(this.powersOf, attempt - 1); //attempt > 0
    return Math.round(step * growBy);
  }

  //fastpath with bitwise operator
  protected long calculatePowerOfTwo(long attempt) {
    long step;
    if (attempt >= 64) { //safeguard against overflow in the bitshift operation
      step = Long.MAX_VALUE;
    } else {
      step = (1L << (attempt - 1));
    }
    //round will cap at Long.MAX_VALUE
    return Math.round(step * growBy);
  }

  private long applyBounds(long calculatedValue) {
    if (calculatedValue < lower) {
      return lower;
    }
    if (calculatedValue > upper) {
      return upper;
    }
    return calculatedValue;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ExponentialDelay{");
    sb.append("growBy ").append(growBy);
    sb.append(" " + unit());
    sb.append(", powers of ").append(powersOf);
    sb.append("; lower=").append(lower);
    sb.append(", upper=").append(upper);
    sb.append('}');
    return sb.toString();
  }
}
