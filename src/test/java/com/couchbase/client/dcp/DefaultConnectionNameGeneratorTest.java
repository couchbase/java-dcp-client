package com.couchbase.client.dcp;

import com.couchbase.client.dcp.core.utils.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultConnectionNameGeneratorTest {

  private final String productName = "MyProduct";

  @Test
  void connectionNameLimitEnforced() throws Exception {
    final String fiveHundredChars = repeat("x", 500);

    final DefaultConnectionNameGenerator generator = DefaultConnectionNameGenerator.forProduct(
        productName, "1.0", fiveHundredChars);

    final String connectionName = generator.name();
    assertEquals(200, connectionName.length());
  }

  @Test
  void userAgentTruncationAccountsForJsonEscapes() throws Exception {
    final String fiveHundredQuotes = repeat("\"", 500);

    final DefaultConnectionNameGenerator generator = DefaultConnectionNameGenerator.forProduct(
        productName, "1.0", fiveHundredQuotes);

    final String connectionName = generator.name();
    assertEquals(199, connectionName.length());
    // Remove product name and : prefix
    final String connectionNameJson = connectionName.substring(productName.length() + 1);

    final Map<String, Object> decoded = DefaultObjectMapper.readValueAsMap(connectionNameJson);
    final String userAgent = (String) decoded.get("a");
    final String expectedQuotes = repeat("\"", 63);
    final String expected = "MyProduct/1.0 (" + expectedQuotes;
    assertEquals(expected, userAgent);
  }

  @Test
  void extractConnectionId() throws Exception {
    final String fiveHundredChars = repeat("x", 500);

    final DefaultConnectionNameGenerator generator = DefaultConnectionNameGenerator.forProduct(
        productName, "1.0", fiveHundredChars);

    final String connectionName = generator.name();
    String connectionId = DefaultConnectionNameGenerator.extractConnectionId(connectionName);
    assertEquals(33, connectionId.length());
    assertTrue(connectionName.contains(connectionId));
  }

  private static String repeat(String s, int count) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(s);
    }
    return sb.toString();
  }
}
