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

import org.junit.Test;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class LinearDelayTest {

  @Test
  public void shouldCalculateLinearly() {
    Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 1);

    assertEquals(1, linearDelay.calculate(1));
    assertEquals(2, linearDelay.calculate(2));
    assertEquals(3, linearDelay.calculate(3));
    assertEquals(4, linearDelay.calculate(4));

    assertEquals("LinearDelay{growBy 1.0 SECONDS; lower=0, upper=2147483647}", linearDelay.toString());
  }

  @Test
  public void shouldRespectLowerBound() {
    Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 3, 1);

    assertEquals(3, linearDelay.calculate(1));
    assertEquals(3, linearDelay.calculate(2));
    assertEquals(3, linearDelay.calculate(3));
    assertEquals(4, linearDelay.calculate(4));

    assertEquals("LinearDelay{growBy 1.0 SECONDS; lower=3, upper=2147483647}", linearDelay.toString());
  }

  @Test
  public void shouldRespectUpperBound() {
    Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, 2, 0, 1);

    assertEquals(1, linearDelay.calculate(1));
    assertEquals(2, linearDelay.calculate(2));
    assertEquals(2, linearDelay.calculate(3));
    assertEquals(2, linearDelay.calculate(4));

    assertEquals("LinearDelay{growBy 1.0 SECONDS; lower=0, upper=2}", linearDelay.toString());
  }

  @Test
  public void shouldApplyFactor() {
    Delay linearDelay = new LinearDelay(TimeUnit.SECONDS, Integer.MAX_VALUE, 0, 0.5);

    assertEquals(1, linearDelay.calculate(1));
    assertEquals(1, linearDelay.calculate(2));
    assertEquals(2, linearDelay.calculate(3));
    assertEquals(2, linearDelay.calculate(4));
    assertEquals(3, linearDelay.calculate(5));
    assertEquals(3, linearDelay.calculate(6));

    assertEquals("LinearDelay{growBy 0.5 SECONDS; lower=0, upper=2147483647}", linearDelay.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfLowerLargerThanUpper() {
    new LinearDelay(TimeUnit.SECONDS, 5, 10, 1);
  }

}
