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

package com.couchbase.client.dcp.message;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Map;

import static com.couchbase.client.dcp.TestHelper.loadHexDumpResource;
import static com.couchbase.client.dcp.core.utils.CbCollections.mapOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ContentAndXattrsTest {
  @Test
  public void canParseExampleXattrs() throws Exception {
    byte[] encodedXattrs = loadHexDumpResource(getClass(), "encoded-xattrs.txt");
    ContentAndXattrs parsed = ContentAndXattrs.parse((byte) DataType.XATTR.bitmask(), encodedXattrs);

    assertArrayEquals(new byte[0], parsed.content());
    assertEquals(exampleXattrs(), parsed.xattrs());
  }

  @Test
  public void canParseWithoutXattrs() throws Exception {
    byte[] content = "{}".getBytes(UTF_8);
    ContentAndXattrs parsed = ContentAndXattrs.parse((byte) 0, content);

    Map<String, String> expectedXattrs = Collections.emptyMap();

    assertArrayEquals(content, parsed.content());
    assertEquals(expectedXattrs, parsed.xattrs());
  }

  @Test
  public void canParseWithXattrsAndContent() throws Exception {
    byte[] content = "{}".getBytes(UTF_8);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    os.write(loadHexDumpResource(getClass(), "encoded-xattrs.txt"));
    os.write(content); // append the document content

    ContentAndXattrs parsed = ContentAndXattrs.parse((byte) DataType.XATTR.bitmask(), os.toByteArray());

    assertArrayEquals(content, parsed.content());
    assertEquals(exampleXattrs(), parsed.xattrs());
  }

  private static Map<String, String> exampleXattrs() {
    return mapOf(
        "_sync", "{\"cas\":\"deadbeefcafefeed\"}",
        "meta", "{\"author\":\"Trond Norbye\",\"content-type\":\"application/octet-stream\"}");
  }
}
