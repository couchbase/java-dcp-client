/*
 * Copyright (c) 2019 Couchbase, Inc.
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

import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.dcp.core.utils.ConnectionString.UnresolvedSocket;
import org.junit.jupiter.api.Test;
import reactor.util.annotation.Nullable;

import java.util.Optional;

import static com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString;
import static com.couchbase.client.dcp.core.utils.CbCollections.listOf;
import static com.couchbase.client.dcp.core.utils.CbCollections.mapOf;
import static com.couchbase.client.dcp.core.utils.ConnectionString.Scheme.COUCHBASE;
import static com.couchbase.client.dcp.core.utils.ConnectionString.Scheme.COUCHBASES;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionStringTest {

  @Test
  void canParseEmptyConnectionString() {
    ConnectionString parsed = ConnectionString.create("");
    assertEquals(COUCHBASE, parsed.scheme());
    assertNull(parsed.username());
    assertTrue(parsed.hosts().isEmpty());
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  void canFormat() {
    String original = "foo,bar:123,zot:456=kv";
    ConnectionString parsed = ConnectionString.create(original);
    assertEquals(ConnectionString.DEFAULT_SCHEME + original, parsed.original());

    original = "foo?one=1&two=2";
    parsed = ConnectionString.create(original);
    assertEquals(ConnectionString.DEFAULT_SCHEME + original, parsed.original());
  }

  @Test
  void isImmutable() {
    ConnectionString cs = ConnectionString.create("couchbase://127.0.0.1?foo=bar");

    assertThrows(UnsupportedOperationException.class, () -> cs.hosts().remove(0));
    assertThrows(UnsupportedOperationException.class, () -> cs.params().remove("foo"));
  }

  @Test
  void withers() {
    ConnectionString cs = ConnectionString.create("couchbase://127.0.0.1?foo=bar");

    assertEquals("couchbases://127.0.0.1?foo=bar", cs.withScheme(COUCHBASES).original());
    assertEquals("couchbase://127.0.0.1?foo=bar", cs.withScheme(COUCHBASES).withScheme(COUCHBASE).original());

    assertEquals("couchbase://127.0.0.1?zot=orb", cs.withParams(mapOf("zot", "orb")).original());
    assertEquals("couchbase://127.0.0.1", cs.withParams(emptyMap()).original());
  }

  @Test
  void canParseUnresolvedSocket() {
    assertParsedUnresolvedSocketMatches("foo", "foo", 0, null);
    assertParsedUnresolvedSocketMatches("foo:123", "foo", 123, null);
    assertParsedUnresolvedSocketMatches("foo:123=kv", "foo", 123, ConnectionString.PortType.KV);
    assertParsedUnresolvedSocketMatches("foo:123=manager", "foo", 123, ConnectionString.PortType.MANAGER);

    assertParsedUnresolvedSocketMatches("[::1]", "0:0:0:0:0:0:0:1", 0, null);
    assertParsedUnresolvedSocketMatches("[::1]:123", "0:0:0:0:0:0:0:1", 123, null);
    assertParsedUnresolvedSocketMatches("[::1]:123=kv", "0:0:0:0:0:0:0:1", 123, ConnectionString.PortType.KV);
    assertParsedUnresolvedSocketMatches("[::1]:123=manager", "0:0:0:0:0:0:0:1", 123, ConnectionString.PortType.MANAGER);

    assertThrows(IllegalArgumentException.class, () -> UnresolvedSocket.parse(""));
    assertThrows(IllegalArgumentException.class, () -> UnresolvedSocket.parse("[]"));
    assertThrows(IllegalArgumentException.class, () -> UnresolvedSocket.parse("foo=kv"));
    assertThrows(IllegalArgumentException.class, () -> UnresolvedSocket.parse("foo:0=kv"));
    assertThrows(IllegalArgumentException.class, () -> UnresolvedSocket.parse("[::1]=kv"));
    assertThrows(IllegalArgumentException.class, () -> UnresolvedSocket.parse("[::1]:0=kv"));
    assertThrows(IllegalArgumentException.class, () -> UnresolvedSocket.parse("[]:0=kv"));
  }

  private static void assertParsedUnresolvedSocketMatches(String s, String expectedHost, int expectedPort, @Nullable ConnectionString.PortType expectedPortType) {
    UnresolvedSocket parsed = UnresolvedSocket.parse(s);
    assertEquals(expectedHost, parsed.host());
    assertEquals(expectedPort, parsed.port());
    assertEquals(Optional.ofNullable(expectedPortType), parsed.portType());
  }

  @Test
  void shouldParseValidSchemes() {
    ConnectionString parsed = ConnectionString.create("couchbase://");
    assertEquals(COUCHBASE, parsed.scheme());
    assertTrue(parsed.hosts().isEmpty());
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbases://");
    assertEquals(COUCHBASES, parsed.scheme());
    assertTrue(parsed.hosts().isEmpty());
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  void shouldFailOnInvalidScheme() {
    assertThrows(CouchbaseException.class, () -> ConnectionString.create("invalid://"));
  }

  @Test
  void shouldParseHostList() {
    ConnectionString parsed = ConnectionString.create("couchbase://localhost");
    assertEquals(COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(1, parsed.hosts().size());
    assertEquals("localhost", parsed.hosts().get(0).hostname());
    assertEquals(0, parsed.hosts().get(0).port());
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://localhost:1234");
    assertEquals(COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(1, parsed.hosts().size());
    assertEquals("localhost", parsed.hosts().get(0).hostname());
    assertEquals(1234, parsed.hosts().get(0).port());
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://foo:1234,bar:5678");
    assertEquals(COUCHBASE, parsed.scheme());
    assertTrue(parsed.params().isEmpty());
    assertEquals(2, parsed.hosts().size());
    assertEquals("foo", parsed.hosts().get(0).hostname());
    assertEquals(1234, parsed.hosts().get(0).port());
    assertEquals("bar", parsed.hosts().get(1).hostname());
    assertEquals(5678, parsed.hosts().get(1).port());
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://foo,bar:5678,baz");
    assertEquals(COUCHBASE, parsed.scheme());
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
  void shouldParseParams() {
    ConnectionString parsed = ConnectionString.create("couchbase://localhost?foo=bar");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals(1, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertNull(parsed.username());

    parsed = ConnectionString.create("couchbase://localhost?foo=bar&setting=true");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals(2, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertEquals("true", parsed.params().get("setting"));
    assertNull(parsed.username());
  }

  @Test
  void trailingParamDelimiterIsOkay() {
    ConnectionString parsed = ConnectionString.create("couchbase://foo?");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertTrue(parsed.params().isEmpty());
    assertNull(parsed.username());
  }

  @Test
  void shouldParseUsername() {
    ConnectionString parsed = ConnectionString.create("couchbase://user@localhost?foo=bar");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals("user", parsed.username());
    assertEquals("localhost", parsed.hosts().get(0).hostname());
    assertEquals(0, parsed.hosts().get(0).port());
    assertEquals(1, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));

    parsed = ConnectionString.create("couchbase://user123@host1,host2?foo=bar&setting=true");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals("user123", parsed.username());
    assertEquals("host1", parsed.hosts().get(0).hostname());
    assertEquals("host2", parsed.hosts().get(1).hostname());
    assertEquals(2, parsed.params().size());
    assertEquals("bar", parsed.params().get("foo"));
    assertEquals("true", parsed.params().get("setting"));
  }

  @Test
  void shouldAcceptSingleIPv6WithoutPort() {
    ConnectionString parsed = ConnectionString.create("couchbase://[::1]");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals("0:0:0:0:0:0:0:1", parsed.hosts().get(0).hostname());
    assertEquals(0, parsed.hosts().get(0).port());
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128]");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals(1, parsed.hosts().size());
    assertEquals("::1/128", parsed.hosts().get(0).hostname());
    assertEquals(0, parsed.hosts().get(0).port());
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  void shouldAcceptMultipleIPv6WithoutPort() {
    ConnectionString parsed = ConnectionString.create("couchbase://[::1], [::1]");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals(2, parsed.hosts().size());
    assertEquals("0:0:0:0:0:0:0:1", parsed.hosts().get(0).hostname());
    assertEquals("0:0:0:0:0:0:0:1", parsed.hosts().get(1).hostname());
    assertTrue(parsed.params().isEmpty());

    parsed = ConnectionString.create("couchbase://[::1/128], [::1/128],[::1/128]");
    assertEquals(COUCHBASE, parsed.scheme());
    assertEquals(3, parsed.hosts().size());
    assertEquals("::1/128", parsed.hosts().get(0).hostname());
    assertEquals("::1/128", parsed.hosts().get(1).hostname());
    assertEquals("::1/128", parsed.hosts().get(2).hostname());
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  void shouldAcceptSingleIPv6WithPort() {
    ConnectionString parsed = ConnectionString.create("couchbases://[::1]:8091, [::1]:11210");
    assertEquals(COUCHBASES, parsed.scheme());
    assertEquals(2, parsed.hosts().size());
    assertEquals("0:0:0:0:0:0:0:1", parsed.hosts().get(0).hostname());
    assertEquals(8091, parsed.hosts().get(0).port());
    assertEquals("0:0:0:0:0:0:0:1", parsed.hosts().get(1).hostname());
    assertEquals(11210, parsed.hosts().get(1).port());
    assertTrue(parsed.params().isEmpty());
  }

  @Test
  void shouldAcceptIpv4WithLcbPortType() {
    ConnectionString parsed = ConnectionString.create("couchbase://foo:1234=http,bar:5678=mcd");
    assertEquals(2, parsed.hosts().size());

    assertEquals("foo", parsed.hosts().get(0).hostname());
    assertEquals(1234, parsed.hosts().get(0).port());
    assertEquals(ConnectionString.PortType.MANAGER, parsed.hosts().get(0).portType().get());

    assertEquals("bar", parsed.hosts().get(1).hostname());
    assertEquals(5678, parsed.hosts().get(1).port());
    assertEquals(ConnectionString.PortType.KV, parsed.hosts().get(1).portType().get());
  }

  @Test
  void shouldAcceptIpv4WithNewPortType() {
    ConnectionString parsed = ConnectionString.create("couchbase://foo:1234=manager,bar:5678=kv");
    assertEquals(2, parsed.hosts().size());

    assertEquals("foo", parsed.hosts().get(0).hostname());
    assertEquals(1234, parsed.hosts().get(0).port());
    assertEquals(ConnectionString.PortType.MANAGER, parsed.hosts().get(0).portType().get());

    assertEquals("bar", parsed.hosts().get(1).hostname());
    assertEquals(5678, parsed.hosts().get(1).port());
    assertEquals(ConnectionString.PortType.KV, parsed.hosts().get(1).portType().get());
  }

  @Test
  void shouldAcceptIPv6WithPortType() {
    ConnectionString parsed = ConnectionString.create("couchbases://[::1]:8091=http,[::1]:11210=kv");
    assertEquals(COUCHBASES, parsed.scheme());
    assertEquals(2, parsed.hosts().size());

    assertEquals("0:0:0:0:0:0:0:1", parsed.hosts().get(0).hostname());
    assertEquals(8091, parsed.hosts().get(0).port());
    assertEquals(ConnectionString.PortType.MANAGER, parsed.hosts().get(0).portType().get());

    assertEquals("0:0:0:0:0:0:0:1", parsed.hosts().get(1).hostname());
    assertEquals(11210, parsed.hosts().get(1).port());
    assertEquals(ConnectionString.PortType.KV, parsed.hosts().get(1).portType().get());

    assertTrue(parsed.params().isEmpty());
  }

  @Test
  void isValidDnsSrv() {
    assertFalse(ConnectionString.create("couchbase://foo,bar").isValidDnsSrv());
    assertFalse(ConnectionString.create("couchbase://").isValidDnsSrv());
    assertFalse(ConnectionString.create("couchbase://127.0.0.1").isValidDnsSrv());
    assertFalse(ConnectionString.create("couchbase://[::1]").isValidDnsSrv());
    assertFalse(ConnectionString.create("").isValidDnsSrv());
    assertTrue(ConnectionString.create("foo").isValidDnsSrv());
    assertTrue(ConnectionString.create("couchbase://foo").isValidDnsSrv());
  }

  @Test
  void gracefullyHandlesInvalidScheme() {
    assertThrows(InvalidArgumentException.class, () -> ConnectionString.create("foo://"));
    assertThrows(InvalidArgumentException.class, () -> ConnectionString.create("://"));
  }

  @Test
  void acceptsMixedCaseSchemes() {
    assertEquals(COUCHBASE, ConnectionString.create("COUCHBASE://").scheme());
    assertEquals(COUCHBASE, ConnectionString.create("cOuChBaSe://").scheme());
    assertEquals(COUCHBASES, ConnectionString.create("cOuChBaSeS://").scheme());
  }


  @Test
  void canCreateFromSeedNodes() {
    ConnectionString connectionString = ConnectionString.create(asConnectionString(listOf(
        SeedNode.create("neither"),
        SeedNode.create("onlyKvPort", Optional.of(123), Optional.empty()),
        SeedNode.create("onlyManagerPort", Optional.empty(), Optional.of(456)),
        SeedNode.create("both", Optional.of(123), Optional.of(456))
    )));

    assertEquals(
        "couchbase://neither,onlyKvPort:123=kv,onlyManagerPort:456=manager,both:123=kv",
        connectionString.original()
    );
  }

}
