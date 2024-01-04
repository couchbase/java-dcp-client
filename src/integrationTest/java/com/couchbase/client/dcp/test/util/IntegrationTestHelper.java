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

package com.couchbase.client.dcp.test.util;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class IntegrationTestHelper {
  private static final Logger log = LoggerFactory.getLogger(IntegrationTestHelper.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  static {
    objectMapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  }

  public static String validateJson(String json) {
    try {
      objectMapper.readTree(json);
      return json;
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Returns the given key with some characters appended so the new key hashes to the desired partition.
   */
  public static Optional<String> forceKeyToPartition(String key, int desiredPartition, int numPartitions) {
    checkArgument(desiredPartition < numPartitions);
    checkArgument(numPartitions > 0);
    checkArgument(desiredPartition >= 0);

    // Why use math when you can apply 💪 BRUTE FORCE!
    final int MAX_ITERATIONS = 10_000_000;

    final MarkableCrc32 crc32 = new MarkableCrc32();
    final byte[] keyBytes = (key + "#").getBytes(UTF_8);
    crc32.update(keyBytes, 0, keyBytes.length);
    crc32.mark();

    for (long salt = 0; salt < MAX_ITERATIONS; salt++) {
      crc32.reset();

      final String saltString = Long.toHexString(salt);
      for (int i = 0, max = saltString.length(); i < max; i++) {
        crc32.update(saltString.charAt(i));
      }

      final long rv = (crc32.getValue() >> 16) & 0x7fff;
      final int actualPartition = (int) rv & numPartitions - 1;

      if (actualPartition == desiredPartition) {
        return Optional.of(new String(keyBytes, UTF_8) + saltString);
      }
    }
    log.warn("Failed to force partition for {} after {} iterations.", key, MAX_ITERATIONS);
    return Optional.empty();
  }
}
