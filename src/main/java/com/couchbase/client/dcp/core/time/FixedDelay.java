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
 * Delay which is fixed for every attempt.
 */
public class FixedDelay extends Delay {

  private final long delay;

  FixedDelay(long delay, TimeUnit unit) {
    super(unit);
    this.delay = delay;
  }

  @Override
  public long calculate(long attempt) {
    return delay;
  }

  @Override
  public String toString() {
    return "FixedDelay{" + delay + " " + unit()+ "}";
  }
}
