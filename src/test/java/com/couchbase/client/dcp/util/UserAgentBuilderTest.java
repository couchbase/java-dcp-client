package com.couchbase.client.dcp.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UserAgentBuilderTest {

  @Test
  public void userAgent() throws Exception {
    String userAgent = new UserAgentBuilder()
        .append("Foo", null)
        .append("Bar", "1.0")
        .append("Zot", "2.0", "parens and backslashes should be escaped ()\\")
        .build();

    assertEquals("Foo Bar/1.0 Zot/2.0 (parens and backslashes should be escaped \\(\\)\\\\)", userAgent);
  }

  @Test
  public void commentsAreJoinedWithSemicolons() throws Exception {
    String userAgent = new UserAgentBuilder()
        .append("Foo", null, "a", "b", "c")
        .build();

    assertEquals("Foo (a; b; c)", userAgent);
  }

  @Test(expected = NullPointerException.class)
  public void productNameMustNotBeNull() throws Exception {
    new UserAgentBuilder().append(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void productNameMustNotBeEmpty() throws Exception {
    new UserAgentBuilder().append("", null);
  }

  @Test
  public void invalidCharsInTokensAreReplacedWithUnderscore() throws Exception {
    String userAgent = new UserAgentBuilder().append("Foo/(Bar\"", "1.0 beta 1").build();
    assertEquals("Foo__Bar_/1.0_beta_1", userAgent);
  }

  @Test
  public void nonAsciiCharactersAreReplaced() throws Exception {
    String userAgent = new UserAgentBuilder().append("Foö", "1.0-béta", "foô").build();
    assertEquals("Fo_/1.0-b_ta (fo?)", userAgent);
  }
}
