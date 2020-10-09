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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.dcp.message.DataType.XATTR;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class ContentAndXattrs {
  private final Map<String, String> xattrs;
  private final byte[] content;

  public ContentAndXattrs(byte[] content, Map<String, String> xattrs) {
    this.content = requireNonNull(content);
    this.xattrs = requireNonNull(xattrs);
  }

  public byte[] content() {
    return content;
  }

  public Map<String, String> xattrs() {
    return xattrs;
  }

  public static ContentAndXattrs parse(byte dataType, byte[] uncompressedContent) {
    if (!DataType.contains(dataType, XATTR)) {
      // Document has no XATTRs, or client was not configured to request them.
      return new ContentAndXattrs(uncompressedContent, emptyMap());
    }

    // Parse the extended attributes. Data format is specified by
    // https://github.com/couchbase/kv_engine/blob/master/docs/Document.md#xattr---extended-attributes
    try (DataInputStream dataInput = new DataInputStream(new ByteArrayInputStream(uncompressedContent))) {
      Map<String, String> xattrs = new HashMap<>();

      // Total length of the extended attributes section (not including this length value itself)
      // Spec says it's an unsigned 32-bit int. In practice it will never be larger than Integer.MAX_VALUE
      // since XATTRs count against the document content size limit of 20 MB.
      final int xattrsLen = dataInput.readInt();
      int bytesRead = 0;
      while (bytesRead < xattrsLen) {
        int keyAndValueLen = dataInput.readInt();
        String key = readNullTerminatedUtf8String(dataInput);
        String value = readNullTerminatedUtf8String(dataInput);
        bytesRead += Integer.BYTES + keyAndValueLen;

        xattrs.put(key, value);
      }

      int contentStart = Integer.BYTES + xattrsLen;
      byte[] content = new byte[uncompressedContent.length - contentStart];
      System.arraycopy(uncompressedContent, contentStart, content, 0, content.length);
      return new ContentAndXattrs(content, xattrs);

    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse extended attributes", e);
    }
  }

  private static String readNullTerminatedUtf8String(DataInput di) throws IOException {
    // The spec for the XATTR format says these values are "modified UTF-8".
    // Modified UTF-8 is typically CESU-8 with nulls encoded as "0xC0 0x80"
    // instead of "0x00". In practice we can parse these values as standard UTF-8,
    // because XATTR keys and values are not allowed to contain null characters.
    //
    // As for characters outside the Basic Multilingial Plane (supplemental characters like
    // emojis, etc.), the server returns them in UTF-8. For example, if you set the value
    // of an XATTR to the bat emoji ("🦇",  U+1F987) encoded as UTF-8, the server returns
    // the same UTF-8 bytes. Judging by this behavior, it's safe to say that "modified UTF-8"
    // in this context does NOT mean CESU-8, since CESU-8 would encode supplementary characters
    // differently than UTF-8.
    //
    // tl;dr In this context, "modified UTF-8" is the same as UTF-8.

    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    byte b;
    while ((b = di.readByte()) != 0) {
      buffer.write(b);
    }
    return buffer.toString("UTF-8");
  }
}
