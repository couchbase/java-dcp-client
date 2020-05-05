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

public class HostAndPort {
  private final String host;
  private final int port;
  private final boolean ipv6Literal;

  public HostAndPort(String host, int port) {
    this.ipv6Literal = host.contains(":");
    this.host = ipv6Literal ? canonicalizeIpv6Literal(host) : host;
    this.port = port;
  }

  private static String canonicalizeIpv6Literal(String ipv6Literal) {
    // This "resolves" the address, but because it's an IPv6 literal no DNS lookup is required
    return new InetSocketAddress("[" + ipv6Literal + "]", 0).getHostString();
  }

  public HostAndPort(InetSocketAddress address) {
    // Don't want reverse DNS lookup, so use getHostString() instead of getHostName()
    this(address.getHostString(), address.getPort());
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

  public InetSocketAddress toAddress() {
    return new InetSocketAddress(host, port);
  }

  public String format() {
    return formatHost() + ":" + port;
  }

  public String formatHost() {
    return ipv6Literal ? "[" + host + "]" : host;
  }

  @Override
  public String toString() {
    return format();
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
    return Objects.hash(host, port);
  }
}
