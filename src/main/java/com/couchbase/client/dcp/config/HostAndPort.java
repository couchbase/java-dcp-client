/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.dcp.config;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

/**
 * A host (hostname or IP address) and a port number.
 */
public class HostAndPort {
  private final String host;
  private final int port;

  // Derived properties, pre-computed and cached for performance.
  private final int hashCode;
  private final String formatted;
  private final String redactedFormatted;

  public HostAndPort(String host, int port) {
    requireNonNull(host, "host must be non-null");

    boolean ipv6Literal = host.contains(":");
    this.host = ipv6Literal ? canonicalizeIpv6Literal(host) : host;
    this.port = port;

    this.hashCode = Objects.hash(this.host, this.port);
    this.formatted = (ipv6Literal ? "[" + this.host + "]" : this.host) + (this.port <= 0 ? "" : ":" + this.port);
    this.redactedFormatted = redactSystem(this.formatted).toString();
  }

  private static String canonicalizeIpv6Literal(String ipv6Literal) {
    // This "resolves" the address, but because it's an IPv6 literal no DNS lookup is required
    return new InetSocketAddress(ipv6Literal, 0).getHostString();
  }

  /**
   * @deprecated Please use {@link #host()} instead.
   */
  @Deprecated
  public String hostname() {
    return host();
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public HostAndPort withPort(int port) {
    return this.port == port ? this : new HostAndPort(this.host, port);
  }

  public String format() {
    return formatted;
  }

  private static final Pattern hostAndPortPattern = Pattern.compile("(?<host>[^:]+)(:(?<port>\\d+))?");
  private static final Pattern hostAndPortPatternIpv6 = Pattern.compile("\\[(?<host>.+)](:(?<port>\\d+))?");

  public static HostAndPort parse(String s) {
    return parse(s, 0);
  }

  public static HostAndPort parse(String s, int defaultPort) {
    s = s.trim();
    Pattern pattern = s.startsWith("[") ? hostAndPortPatternIpv6 : hostAndPortPattern;
    Matcher m = pattern.matcher(s);
    if (!m.matches()) {
      throw new IllegalArgumentException("Malformed address: " + s);
    }

    String portString = m.group("port");
    int port = portString == null ? defaultPort : Integer.parseInt(portString);
    return new HostAndPort(m.group("host"), port);
  }

  @Override
  public String toString() {
    return redactedFormatted;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HostAndPort that = (HostAndPort) o;
    return port == that.port &&
        host.equals(that.host);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
