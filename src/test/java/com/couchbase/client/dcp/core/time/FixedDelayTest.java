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

public class FixedDelayTest {

  @Test
  void shouldCalculateFixedDelay() {
    Delay fixedDelay = new FixedDelay(3, TimeUnit.SECONDS);

    assertEquals(3, fixedDelay.calculate(1));
    assertEquals(3, fixedDelay.calculate(2));
    assertEquals(3, fixedDelay.calculate(3));

    assertEquals("FixedDelay{3 SECONDS}", fixedDelay.toString());
  }

}
