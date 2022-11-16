/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBufUtil;
import org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TestHelper {
  private TestHelper() {
    throw new AssertionError("not instantiable");
  }

  public static byte[] loadResource(Class<?> loader, String resourceName) throws IOException {
    try (InputStream is = loader.getResourceAsStream(resourceName)) {
      if (is == null) {
        throw new FileNotFoundException("Failed to locate resource '" + resourceName + "' relative to " + loader);
      }
      return IOUtils.toByteArray(is);
    }
  }

  public static String loadStringResource(Class<?> loader, String resourceName) throws IOException {
    return new String(loadResource(loader, resourceName), UTF_8);
  }

  public static byte[] loadHexDumpResource(Class<?> loader, String resourceName) throws IOException {
    return decodePrettyHexDump(loadStringResource(loader, resourceName));
  }

  /**
   * Helper method which very simply strips apart the pretty hex dump
   * format and turns it into something useful for testing.
   */
  public static byte[] decodePrettyHexDump(final String dump) {
    StringBuilder rawDump = new StringBuilder();
    for (String line : dump.split("\\r?\\n")) {
      if (line.startsWith("|")) {
        String[] parts = line.split("\\|");
        String sequence = parts[2];
        String stripped = sequence.replaceAll("\\s", "");
        rawDump.append(stripped);
      }
    }
    return ByteBufUtil.decodeHexDump(rawDump);
  }
}
