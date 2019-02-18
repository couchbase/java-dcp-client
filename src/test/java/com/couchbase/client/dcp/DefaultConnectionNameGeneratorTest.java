package com.couchbase.client.dcp;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultConnectionNameGeneratorTest {

    @Test
    public void userAgentTruncationAccountsForJsonEscapes() {
        final String fiveHundredQuotes = repeat("\"", 500);

        final DefaultConnectionNameGenerator generator = DefaultConnectionNameGenerator.forProduct(
                "MyProduct", "1.0", fiveHundredQuotes);

        final String connectionName = generator.name();

        final String prefix = connectionName.substring(0, 207);
        final String ninetyTwoEscapedQuotes = repeat("\\\"", 92);
        final String expected = "{\"a\":\"MyProduct/1.0 (" + ninetyTwoEscapedQuotes + "\",";
        assertEquals(expected, prefix);
    }

    private static String repeat(String s, int count) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(s);
        }
        return sb.toString();
    }
}
