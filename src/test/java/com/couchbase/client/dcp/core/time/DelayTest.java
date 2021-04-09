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

public class DelayTest {

  @Test
  void shouldBuildFixedDelay() {
    Delay delay = Delay.fixed(5, TimeUnit.MICROSECONDS);
    assertEquals(TimeUnit.MICROSECONDS, delay.unit());
    assertEquals(5, delay.calculate(10));
  }

  @Test
  void shouldBuildLinearDelay() {
    Delay delay = Delay.linear(TimeUnit.HOURS);
    assertEquals(TimeUnit.HOURS, delay.unit());
    assertEquals(10, delay.calculate(10));
  }

  @Test
  void shouldBuildExponentialDelay() {
    Delay delay = Delay.exponential(TimeUnit.SECONDS);
    assertEquals(TimeUnit.SECONDS, delay.unit());
    assertEquals(512, delay.calculate(10));
  }

}
