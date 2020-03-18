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
package com.couchbase.client.dcp.core.utils;

import com.couchbase.client.dcp.core.CouchbaseException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectionStringTest {

  @Test
  public void shouldParseValidSchemes() {
    ConnectionString parsed = ConnectionString.create("couchbase://");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.hosts().isEmpty());
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbases://");
    assertEquals(ConnectionString.Scheme.COUCHBASES, parsed.scheme());
    assertTrue(parsed.hosts().isEmpty());
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("http://");
    assertEquals(ConnectionString.Scheme.HTTP, parsed.scheme());
    assertTrue(parsed.hosts().isEmpty());
    assertTrue(parsed.params().isEmpty());
  }

  @Test(expected = CouchbaseException.class)
  public void shouldFailOnInvalidScheme() {
    ConnectionString.create("invalid://");
  }

  @Test
  public void shouldParseHostList() {
    ConnectionString parsed = ConnectionString.create("couchbase://localhost");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(1, parsed.hosts().size());
    assertEquals("localhost", parsed.hosts().get(0).hostname());
    assertEquals(0, parsed.hosts().get(0).port());
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://localhost:1234");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(1, parsed.hosts().size());
    assertEquals("localhost", parsed.hosts().get(0).hostname());
    assertEquals(1234, parsed.hosts().get(0).port());
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://foo:1234,bar:5678");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(2, parsed.hosts().size());
    assertEquals("foo", parsed.hosts().get(0).hostname());
    assertEquals(1234, parsed.hosts().get(0).port());
    assertEquals("bar", parsed.hosts().get(1).hostname());
    assertEquals(5678, parsed.hosts().get(1).port());
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://foo,bar:5678,baz");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(3, parsed.hosts().size());
    assertEquals("foo", parsed.hosts().get(0).hostname());
    assertEquals(0, parsed.hosts().get(0).port());
    assertEquals("bar", parsed.hosts().get(1).hostname());
    assertEquals(5678, parsed.hosts().get(1).port());
    assertEquals("baz", parsed.hosts().get(2).hostname());
    assertEquals(0, parsed.hosts().get(2).port());
    assertNull(parsed.username());
  }

  @Test
  public void shouldParseParams() {
    ConnectionString parsed = ConnectionString.create("couchbase://localhost?foo=bar");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals(1, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://localhost?foo=bar&setting=true");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals(2, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertEquals("true", parsed.params().get("setting"));
    assertNull(parsed.username());
  }

  @Test
  public void shouldParseUsername() {
    ConnectionString parsed = ConnectionString.create("couchbase://user@localhost?foo=bar");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals("user", parsed.username());
    assertEquals(new ConnectionString.UnresolvedSocket("localhost", 0), parsed.hosts().get(0));
    assertEquals(1, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));

    parsed = ConnectionString.create("couchbase://user123@host1,host2?foo=bar&setting=true");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals("user123", parsed.username());
    assertEquals(new ConnectionString.UnresolvedSocket("host1", 0), parsed.hosts().get(0));
    assertEquals(new ConnectionString.UnresolvedSocket("host2", 0), parsed.hosts().get(1));
    assertEquals(2, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertEquals("true", parsed.params().get("setting"));
  }

  @Test
  public void shouldAcceptSingleIPv6WithoutPort() {
    ConnectionString parsed = ConnectionString.create("couchbase://[::1]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals(new ConnectionString.UnresolvedSocket("::1", 0), parsed.hosts().get(0));
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals(new ConnectionString.UnresolvedSocket("::1/128", 0), parsed.hosts().get(0));
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  public void shouldAcceptMultipleIPv6WithoutPort() {
    ConnectionString parsed = ConnectionString.create("couchbase://[::1], [::1]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(2, parsed.hosts().size());
    assertEquals(new ConnectionString.UnresolvedSocket("::1", 0), parsed.hosts().get(0));
    assertEquals(new ConnectionString.UnresolvedSocket("::1", 0), parsed.hosts().get(1));
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128], [::1/128],[::1/128], [2001:DB4:BAD:A55::5]");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(4, parsed.hosts().size());
    assertEquals(new ConnectionString.UnresolvedSocket("::1/128", 0), parsed.hosts().get(0));
    assertEquals(new ConnectionString.UnresolvedSocket("::1/128", 0), parsed.hosts().get(1));
    assertEquals(new ConnectionString.UnresolvedSocket("::1/128", 0), parsed.hosts().get(2));
    assertEquals(new ConnectionString.UnresolvedSocket("2001:DB4:BAD:A55::5", 0), parsed.hosts().get(3));
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  public void shouldAcceptSingleIPv6WithPort() {
    ConnectionString parsed = ConnectionString.create("couchbases://[::1]:8091, [::1]:11210");
    assertEquals(ConnectionString.Scheme.COUCHBASES, parsed.scheme());
    assertEquals(2, parsed.hosts().size());
    assertEquals(new ConnectionString.UnresolvedSocket("::1", 8091), parsed.hosts().get(0));
    assertEquals(new ConnectionString.UnresolvedSocket("::1", 11210), parsed.hosts().get(1));
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128]:1234, [::1/128]:11210,[::1/128]:1, [2001:DB4:BAD:A55::5]:111");
    assertEquals(ConnectionString.Scheme.COUCHBASE, parsed.scheme());
    assertEquals(4, parsed.hosts().size());
    assertEquals(new ConnectionString.UnresolvedSocket("::1/128", 1234), parsed.hosts().get(0));
    assertEquals(new ConnectionString.UnresolvedSocket("::1/128", 11210), parsed.hosts().get(1));
    assertEquals(new ConnectionString.UnresolvedSocket("::1/128", 1), parsed.hosts().get(2));
    assertEquals(new ConnectionString.UnresolvedSocket("2001:DB4:BAD:A55::5", 111), parsed.hosts().get(3));
    assertTrue(parsed.params().isEmpty());
  }

}
