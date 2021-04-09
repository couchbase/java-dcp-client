/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.config;

import com.couchbase.client.dcp.util.Version;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static com.couchbase.client.dcp.config.CompressionMode.DISABLED;
import static com.couchbase.client.dcp.config.CompressionMode.ENABLED;
import static com.couchbase.client.dcp.config.CompressionMode.FORCED;
import static com.couchbase.client.dcp.message.HelloFeature.DATATYPE;
import static com.couchbase.client.dcp.message.HelloFeature.SNAPPY;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompressionModeTest {
  private static final Version OLDER = new Version(4, 0, 0);
  private static final Version WATSON = new Version(4, 5, 0);
  private static final Version VULCAN = new Version(5, 5, 0);

  @Test
  void effectiveMode() throws Exception {
    assertEquals(DISABLED, CompressionMode.DISABLED.effectiveMode(OLDER));
    assertEquals(DISABLED, CompressionMode.DISABLED.effectiveMode(WATSON));
    assertEquals(DISABLED, CompressionMode.DISABLED.effectiveMode(VULCAN));

    assertEquals(DISABLED, FORCED.effectiveMode(OLDER));
    assertEquals(DISABLED, FORCED.effectiveMode(WATSON));
    assertEquals(FORCED, FORCED.effectiveMode(VULCAN));

    assertEquals(DISABLED, ENABLED.effectiveMode(OLDER));
    assertEquals(DISABLED, ENABLED.effectiveMode(WATSON));
    assertEquals(ENABLED, ENABLED.effectiveMode(VULCAN));
  }

  @Test
  void vulcanDisabled() throws Exception {
    assertEquals(emptySet(), DISABLED.getHelloFeatures(VULCAN));
    assertEquals(emptyMap(), DISABLED.getDcpControls(VULCAN));
  }

  @Test
  void vulcanForced() throws Exception {
    assertEquals(EnumSet.of(DATATYPE, SNAPPY), FORCED.getHelloFeatures(VULCAN));
    assertEquals(singletonMap("force_value_compression", "true"), FORCED.getDcpControls(VULCAN));
  }

  @Test
  void vulcanDiscretionary() throws Exception {
    assertEquals(EnumSet.of(DATATYPE, SNAPPY), ENABLED.getHelloFeatures(VULCAN));
    assertEquals(emptyMap(), ENABLED.getDcpControls(VULCAN));
  }

  @Test
  void watsonDisabled() throws Exception {
    // Don't care about hello, doesn't affect server compression behavior
    assertEquals(emptyMap(), DISABLED.getDcpControls(WATSON));
  }

  @Test
  void watsonForced() throws Exception {
    assertThrows(IllegalArgumentException.class, () -> FORCED.getDcpControls(WATSON));
  }

  @Test
  void olderDisabled() throws Exception {
    // Don't care about hello, doesn't affect server compression behavior
    assertEquals(emptyMap(), DISABLED.getDcpControls(OLDER));
  }
}
