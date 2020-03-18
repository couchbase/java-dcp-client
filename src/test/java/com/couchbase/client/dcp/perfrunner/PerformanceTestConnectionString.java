/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.perfrunner;

import com.couchbase.client.dcp.core.CouchbaseException;
import com.couchbase.client.dcp.core.utils.ConnectionString.Scheme;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An Couchbase connection string with support for password and bucket name.
 */
class PerformanceTestConnectionString {
  private static final Pattern IPV6_PATTERN = Pattern.compile("^\\[(.+)](:\\d+)?$");

  private final Scheme scheme;
  private final String username;
  private final String password;
  private final List<InetSocketAddress> hosts;
  private final String bucket;
  private final Map<String, String> params;

  PerformanceTestConnectionString(final String input) {
    GenericUri uri = new GenericUri(input);

    this.scheme = parseScheme(uri.scheme());

    String authority = uri.authority(); // hosts OR username@hosts OR username:password@hosts
    int userInfoDelimeterIndex = authority.indexOf("@");
    if (userInfoDelimeterIndex == -1) {
      this.username = null;
      this.password = null;
      this.hosts = parseHosts(authority);
    } else {
      String userInfo = authority.substring(0, userInfoDelimeterIndex);
      String[] usernameAndPassword = userInfo.split(":", 2);
      this.username = decode(usernameAndPassword[0]);
      this.password = usernameAndPassword.length == 1 ? null : decode(usernameAndPassword[1]);
      this.hosts = parseHosts(authority.substring(userInfoDelimeterIndex + 1));
    }

    this.bucket = uri.path().isEmpty() ? null : decode(uri.path().substring(1));
    this.params = parseParams(uri.query());
  }

  private static Scheme parseScheme(final String input) {
    try {
      return Scheme.valueOf(input.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new CouchbaseException("Could not parse Scheme of connection string: " + input);
    }
  }

  private static List<InetSocketAddress> parseHosts(final String input) {
    try {
      List<InetSocketAddress> hosts = new ArrayList<>();

      String[] hostStrings = input.split(",");
      for (String singleHost : hostStrings) {
        singleHost = singleHost.trim();
        if (singleHost.isEmpty()) {
          continue;
        }
        hosts.add(parseOneAddress(singleHost));

      }

      return hosts;

    } catch (Exception e) {
      throw new CouchbaseException("Failed to parse connection string host component: " + input, e);
    }
  }

  private static InetSocketAddress parseOneAddress(String address) {
    Matcher matcher = IPV6_PATTERN.matcher(address);
    if (matcher.matches()) {
      // this is an ipv6 addr!
      String host = matcher.group(1);
      String portString = matcher.group(2);
      int port = portString == null ? 0 : Integer.parseInt(portString.substring(1));
      return new InetSocketAddress(host, port);
    }

    // either ipv4 or a hostname
    String[] hostAndPort = address.split(":");
    int port = hostAndPort.length == 1 ? 0 : Integer.parseInt(hostAndPort[1]);
    return new InetSocketAddress(hostAndPort[0], port);
  }

  private static Map<String, String> parseParams(String input) {
    Map<String, String> params = new HashMap<>();
    if (input == null) {
      return params;
    }

    String[] exploded = input.split("&");
    for (String anExploded : exploded) {
      String[] pair = anExploded.split("=", 2);
      params.put(decodeInQueryString(pair[0]), pair.length == 1 ? "" : decodeInQueryString(pair[1]));
    }

    return params;
  }

  public Scheme scheme() {
    return scheme;
  }

  /**
   * Returns the username, or {@code null} if not present.
   */
  public String username() {
    return username;
  }

  /**
   * Returns the password, or {@code null} if not present.
   */
  public String password() {
    return password;
  }

  /**
   * Returns the list of hosts (possibly empty, but never {@code null}).
   */
  public List<InetSocketAddress> hosts() {
    return hosts;
  }

  /**
   * Returns the bucket name, or {@code null} if not present.
   */
  public String bucket() {
    return bucket;
  }

  /**
   * Returns the map of parameters (possibly empty, but never {@code null}).
   * Keys and values are guaranteed to be non-null.
   */
  public Map<String, String> params() {
    return params;
  }

  private static String decode(String percentEncoded) {
    // Replace so decodeInQueryString doesn't convert plus (+) to space
    final String percentEncodedPlus = "%2B";
    return decodeInQueryString(percentEncoded.replace("+", percentEncodedPlus));
  }

  private static String decodeInQueryString(String percentEncoded) {
    try {
      // treats "+" as an encoded space.
      return URLDecoder.decode(percentEncoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e); // UTF-8 is always supported
    }
  }

  @Override
  public String toString() {
    return "PerformanceTestConnectionString{" +
        "scheme=" + scheme +
        ", username='" + username + '\'' +
        ", hasPassword='" + (password != null) + '\'' +
        ", hosts=" + hosts +
        ", bucket='" + bucket + '\'' +
        ", params=" + params +
        '}';
  }
}
