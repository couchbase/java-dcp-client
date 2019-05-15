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
package com.couchbase.client.dcp;

import com.couchbase.client.core.utils.DefaultObjectMapper;
import com.couchbase.client.dcp.util.UserAgentBuilder;
import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.couchbase.client.dcp.ClientVersion.clientVersion;

/**
 * The default implementation for the {@link ConnectionNameGenerator}.
 * <p>
 * It generates a new name every time called, using the format specified by the
 * <a href="https://github.com/couchbaselabs/sdk-rfcs/blob/master/rfc/0035-rto.md#client--connection-ids">
 * Response Time Observability RFC</a>.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DefaultConnectionNameGenerator implements ConnectionNameGenerator {
  /**
   * A connection name generator with a default User Agent string.
   */
  public static final ConnectionNameGenerator INSTANCE = new DefaultConnectionNameGenerator(new UserAgentBuilder());

  private static final String clientId = paddedHex(randomLong());

  private final String userAgent;

  /**
   * Returns a new connection name generator that includes the given product information in the User Agent string.
   *
   * @param productName Product name to include in the User Agent string.
   * @param productVersion Optional product version to include in the User Agent string. May be null.
   * @param comments Optional comments to include in the User Agent string.
   */
  public static DefaultConnectionNameGenerator forProduct(String productName, String productVersion, String... comments) {
    return new DefaultConnectionNameGenerator(
        new UserAgentBuilder().append(productName, productVersion, comments));
  }

  private DefaultConnectionNameGenerator(UserAgentBuilder userAgentBuilder) {
    userAgentBuilder
        .append("java-dcp-client", clientVersion())
        .appendJava()
        .appendOs();

    // The JSON form of the user agent string may use no more than 202 of the maximum key size of 250 bytes.
    // That's 200 bytes for the user agent (including any JSON escape chars) and 2 bytes for the enclosing quotes.
    // We can assume 1 byte per character, since the User Agent builder only outputs ASCII characters.
    //
    // HOWEVER! Let's work around a bug Couchbase Server 6.0.0 (and possibly other versions) where rebalance fails
    // if the name is too long (see JDCP-126). We don't really *need* the Java and OS info anyway.
    final boolean WORKAROUND_CBSE_6804 = true;
    final int userAgentMaxLength = WORKAROUND_CBSE_6804 ? 102 : 202;
    this.userAgent = truncateAsJson(userAgentBuilder.build(), userAgentMaxLength);
  }

  @Override
  public String name() {
    final String connectionId = paddedHex(randomLong());

    // Output the connection ID first so it's more likely to be retained
    // if someone needs to truncate the name for whatever reason.
    final Map<String, String> name = new LinkedHashMap<>();
    name.put("i", clientId + "/" + connectionId);
    name.put("a", userAgent);

    try {
      return DefaultObjectMapper.writeValueAsString(name);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns the given string truncated so its JSON representation does not exceed the given length.
   * <p>
   * Assumes the input contains only printable characters, with whitespace restricted to spaces and horizontal tabs.
   */
  private static String truncateAsJson(String s, int maxSerializedLength) {
    int resultLength = 0;
    int spaceLeft = maxSerializedLength - 2; // enclosing quotes always consume 2 slots

    for (char c : s.toCharArray()) {
      final boolean charNeedsEscape = c == '\\' || c == '"' || c == '\t';

      spaceLeft -= charNeedsEscape ? 2 : 1;
      if (spaceLeft < 0) {
        break;
      }
      resultLength++;
    }

    return truncate(s, resultLength);
  }

  private static String truncate(String s, int maxLength) {
    return s.length() <= maxLength ? s : s.substring(0, maxLength);
  }

  private static String paddedHex(long number) {
    return String.format("%016X", number);
  }

  private static long randomLong() {
    return ThreadLocalRandom.current().nextLong();
  }
}
