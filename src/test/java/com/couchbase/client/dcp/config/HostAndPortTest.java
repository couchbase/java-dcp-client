package com.couchbase.client.dcp.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class HostAndPortTest {
  @Test
  public void ipv6LiteralsAreCanonicalized() throws Exception {
    assertEquals("0:0:0:0:0:0:0:1", new HostAndPort("::1", 0).host());
    assertEquals("0:0:0:0:0:0:0:a", new HostAndPort("::A", 0).host());
  }

  @Test
  public void equalsUsesCanonicalHost() throws Exception {
    assertEquals(new HostAndPort("0:0:0:0:0:0:0:1", 0), new HostAndPort("::1", 0));
    assertEquals(new HostAndPort("0:0:0:0:0:0:0:a", 0), new HostAndPort("::A", 0));
  }

  @Test
  public void equalsUsesUnresolvedNames() throws Exception {
    assertNotEquals(new HostAndPort("localhost", 0), new HostAndPort("127.0.0.1", 0));
    assertNotEquals(new HostAndPort("localhost", 0), new HostAndPort("::1", 0));
  }

  @Test
  public void format() throws Exception {
    assertEquals("127.0.0.1:12345", new HostAndPort("127.0.0.1", 12345).format());
    assertEquals("[0:0:0:0:0:0:0:1]:12345", new HostAndPort("0:0:0:0:0:0:0:1", 12345).format());
    assertEquals("example.com:12345", new HostAndPort("example.com", 12345).format());
  }
}
