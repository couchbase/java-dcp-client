/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp;

import java.util.concurrent.ThreadLocalRandom;

public class ConnectionIdGenerator {
  private static final String clientId = paddedHex(randomLong());

  public String next() {
    return clientId + "/" + paddedHex(randomLong());
  }

  private static String paddedHex(long number) {
    return String.format("%016X", number);
  }

  private static long randomLong() {
    return ThreadLocalRandom.current().nextLong();
  }
}
