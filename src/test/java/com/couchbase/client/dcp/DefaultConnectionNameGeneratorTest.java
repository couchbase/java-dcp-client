package com.couchbase.client.dcp;

import com.couchbase.client.core.utils.DefaultObjectMapper;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DefaultConnectionNameGeneratorTest {

  @Test
  public void userAgentTruncationAccountsForJsonEscapes() throws Exception {
    final String fiveHundredQuotes = repeat("\"", 500);

    final DefaultConnectionNameGenerator generator = DefaultConnectionNameGenerator.forProduct(
        "MyProduct", "1.0", fiveHundredQuotes);

    final String connectionName = generator.name();

    final Map<String, Object> decoded = DefaultObjectMapper.readValueAsMap(connectionName);
    final String userAgent = (String) decoded.get("a");
    final String expectedQuotes = repeat("\"", 42);
    final String expected = "MyProduct/1.0 (" + expectedQuotes;
    assertEquals(expected, userAgent);
  }

  private static String repeat(String s, int count) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(s);
    }
    return sb.toString();
  }
}
