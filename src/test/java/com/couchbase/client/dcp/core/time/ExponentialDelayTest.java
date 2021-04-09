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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExponentialDelayTest {

  @Test
  void testPowerOfTwoShouldCalculateExponentially() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 1, 2);

    assertEquals(0, exponentialDelay.calculate(0));
    assertEquals(1, exponentialDelay.calculate(1));
    assertEquals(2, exponentialDelay.calculate(2));
    assertEquals(4, exponentialDelay.calculate(3));
    assertEquals(8, exponentialDelay.calculate(4));
    assertEquals(16, exponentialDelay.calculate(5));
    assertEquals(32, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 2; lower=0, upper=2147483647}", exponentialDelay.toString());

  }

  @Test
  void testPowerOfTwoShouldRespectLowerBound() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 10, 1, 2);

    assertEquals(10, exponentialDelay.calculate(0));
    assertEquals(10, exponentialDelay.calculate(1));
    assertEquals(10, exponentialDelay.calculate(2));
    assertEquals(10, exponentialDelay.calculate(3));
    assertEquals(10, exponentialDelay.calculate(4));
    assertEquals(16, exponentialDelay.calculate(5));
    assertEquals(32, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 2; lower=10, upper=2147483647}", exponentialDelay.toString());
  }

  @Test
  void testPowerOfTwoShouldRespectUpperBound() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, 9, 0, 1, 2);

    assertEquals(0, exponentialDelay.calculate(0));
    assertEquals(1, exponentialDelay.calculate(1));
    assertEquals(2, exponentialDelay.calculate(2));
    assertEquals(4, exponentialDelay.calculate(3));
    assertEquals(8, exponentialDelay.calculate(4));
    assertEquals(9, exponentialDelay.calculate(5));
    assertEquals(9, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 2; lower=0, upper=9}", exponentialDelay.toString());
  }

  @Test
  void testPowerOfTwoShouldApplyFactor() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 10, 2);

    assertEquals(0, exponentialDelay.calculate(0));
    assertEquals(10, exponentialDelay.calculate(1));
    assertEquals(20, exponentialDelay.calculate(2));
    assertEquals(40, exponentialDelay.calculate(3));
    assertEquals(80, exponentialDelay.calculate(4));
    assertEquals(160, exponentialDelay.calculate(5));
    assertEquals(320, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 10.0 SECONDS, powers of 2; lower=0, upper=2147483647}", exponentialDelay.toString());
  }

  @Test
  void testPowerOfTwoShouldNotOverflowInAMillionRetries() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Long.MAX_VALUE, 0, 2d, 2);

    long previous = Long.MIN_VALUE;
    //the bitwise operation in ExponentialDelay would overflow the step at i = 32
    for (int i = 0; i < 1000000; i++) {
      long now = exponentialDelay.calculate(i);
      assertTrue(now >= previous, "delay is at " + now + " down from " + previous + ", attempt " + i);
      previous = now;
    }
  }

  @Test
  void testPowerOfTenShouldCalculateExponentially() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 1, 10);

    assertEquals(0, exponentialDelay.calculate(0));
    assertEquals(1, exponentialDelay.calculate(1)); //1 * 10^0
    assertEquals(10, exponentialDelay.calculate(2)); //1 * 10^1
    assertEquals(100, exponentialDelay.calculate(3)); //1 * 10^2
    assertEquals(1000, exponentialDelay.calculate(4));
    assertEquals(10000, exponentialDelay.calculate(5));
    assertEquals(100000, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 10; lower=0, upper=2147483647}", exponentialDelay.toString());
  }

  @Test
  void testPowerOfTenShouldRespectLowerBound() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 400, 1, 10);

    assertEquals(400, exponentialDelay.calculate(0));
    assertEquals(400, exponentialDelay.calculate(1));
    assertEquals(400, exponentialDelay.calculate(2));
    assertEquals(400, exponentialDelay.calculate(3));
    assertEquals(1000, exponentialDelay.calculate(4));
    assertEquals(10000, exponentialDelay.calculate(5));
    assertEquals(100000, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 10; lower=400, upper=2147483647}", exponentialDelay.toString());
  }

  @Test
  void testPowerOfTenShouldRespectUpperBound() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, 500, 0, 1, 10);

    assertEquals(0, exponentialDelay.calculate(0));
    assertEquals(1, exponentialDelay.calculate(1));
    assertEquals(10, exponentialDelay.calculate(2));
    assertEquals(100, exponentialDelay.calculate(3));
    assertEquals(500, exponentialDelay.calculate(4));
    assertEquals(500, exponentialDelay.calculate(5));
    assertEquals(500, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 1.0 SECONDS, powers of 10; lower=0, upper=500}", exponentialDelay.toString());
  }

  @Test
  void testPowerOfTenShouldApplyFactor() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 2, 10);

    assertEquals(0, exponentialDelay.calculate(0));
    assertEquals(2, exponentialDelay.calculate(1));
    assertEquals(20, exponentialDelay.calculate(2));
    assertEquals(200, exponentialDelay.calculate(3));
    assertEquals(2000, exponentialDelay.calculate(4));
    assertEquals(20000, exponentialDelay.calculate(5));
    assertEquals(200000, exponentialDelay.calculate(6));

    assertEquals("ExponentialDelay{growBy 2.0 SECONDS, powers of 10; lower=0, upper=2147483647}", exponentialDelay.toString());
  }

  @Test
  void testPowerOfTenShouldNotOverflowInAMillionRetries() {
    Delay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Long.MAX_VALUE, 0, 2d, 10);

    long previous = Long.MIN_VALUE;
    for (int i = 0; i < 1000000; i++) {
      long now = exponentialDelay.calculate(i);
      assertTrue(now >= previous, "delay is at " + now + " down from " + previous + ", attempt " + i);
      previous = now;
    }
  }

  @Test
  void testAlternatePowerAndBitshiftPowerProduceSameResult() {
    ExponentialDelay exponentialDelay = new ExponentialDelay(TimeUnit.SECONDS, Long.MAX_VALUE, 0, 1, 2);

    for (int i = 1; i < 1000000; i++) {
      long powValue = exponentialDelay.calculateAlternatePower(i);
      long bitshiftValue = exponentialDelay.calculatePowerOfTwo(i);

      assertEquals(powValue, bitshiftValue, "difference at step " + i);
    }
  }

  @Test
  void shouldFailIfLowerLargerThanUpper() {
    assertThrows(IllegalArgumentException.class, () -> new ExponentialDelay(TimeUnit.SECONDS, 5, 10, 1, 2));
  }

}
