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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.meta;
import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.core.logging.RedactableArgument.user;

public class ConnectionString {

  public static final String DEFAULT_SCHEME = "couchbase://";

  private final Scheme scheme;
  private final List<UnresolvedSocket> hosts;
  private final Map<String, String> params;
  private final String username;

  protected ConnectionString(final String input) {
    this.scheme = parseScheme(input);
    this.username = parseUser(input);
    this.hosts = parseHosts(input);
    this.params = parseParams(input);
  }

  public static ConnectionString create(final String input) {
    return new ConnectionString(input);
  }

  public static ConnectionString fromHostnames(final List<String> hostnames) {
    StringBuilder sb = new StringBuilder(DEFAULT_SCHEME);
    for (int i = 0; i < hostnames.size(); i++) {
      sb.append(hostnames.get(i));
      if (i < hostnames.size() - 1) {
        sb.append(",");
      }
    }
    return create(sb.toString());
  }

  static Scheme parseScheme(final String input) {
    if (input.startsWith("couchbase://")) {
      return Scheme.COUCHBASE;
    } else if (input.startsWith("couchbases://")) {
      return Scheme.COUCHBASES;
    } else if (input.startsWith("http://")) {
      return Scheme.HTTP;
    } else {
      throw new CouchbaseException("Could not parse Scheme of connection string: " + input);
    }
  }

  static String parseUser(final String input) {
    if (!input.contains("@")) {
      return null;
    } else {
      String schemeRemoved = input.replaceAll("\\w+://", "");
      String username = schemeRemoved.replaceAll("@.*", "");
      return username;
    }
  }

  static List<UnresolvedSocket> parseHosts(final String input) {
    String schemeRemoved = input.replaceAll("\\w+://", "");
    String usernameRemoved = schemeRemoved.replaceAll(".*@", "");
    String paramsRemoved = usernameRemoved.replaceAll("\\?.*", "");
    String[] splitted = paramsRemoved.split(",");

    List<UnresolvedSocket> hosts = new ArrayList<>();

    Pattern ipv6pattern = Pattern.compile("^\\[(.+)]:(\\d+)$");
    for (int i = 0; i < splitted.length; i++) {
      String singleHost = splitted[i];
      if (singleHost == null || singleHost.isEmpty()) {
        continue;
      }
      singleHost = singleHost.trim();

      Matcher matcher = ipv6pattern.matcher(singleHost);
      if (singleHost.startsWith("[") && singleHost.endsWith("]")) {
        // this is an ipv6 addr!
        singleHost = singleHost.substring(1, singleHost.length() - 1);
        hosts.add(new UnresolvedSocket(singleHost, 0));
      } else if (matcher.matches()) {
        // this is ipv6 with addr and port!
        hosts.add(new UnresolvedSocket(
            matcher.group(1),
            Integer.parseInt(matcher.group(2)))
        );
      } else {
        // either ipv4 or a hostname
        String[] parts = singleHost.split(":");
        if (parts.length == 1) {
          hosts.add(new UnresolvedSocket(parts[0], 0));
        } else {
          hosts.add(new UnresolvedSocket(parts[0], Integer.parseInt(parts[1])));
        }
      }
    }
    return hosts;
  }

  static Map<String, String> parseParams(final String input) {
    try {
      String[] parts = input.split("\\?");
      Map<String, String> params = new HashMap<String, String>();
      if (parts.length > 1) {
        String found = parts[1];
        String[] exploded = found.split("&");
        for (int i = 0; i < exploded.length; i++) {
          String[] pair = exploded[i].split("=");
          params.put(pair[0], pair[1]);
        }
      }
      return params;
    } catch (Exception ex) {
      throw new CouchbaseException("Could not parse Params of connection string: " + input, ex);
    }
  }

  public Scheme scheme() {
    return scheme;
  }

  public String username() {
    return username;
  }

  /**
   * Get the list of hosts that could be resolved
   *
   * @return hosts
   */
  public List<UnresolvedSocket> hosts() {
    return hosts;
  }

  public Map<String, String> params() {
    return params;
  }

  /**
   * Get the list of all hosts set on the connection string.
   *
   * @return hosts
   * @deprecated use {@link #hosts()}
   */
  public List<UnresolvedSocket> allHosts() {
    return hosts();
  }

  public enum Scheme {
    HTTP,
    COUCHBASE,
    COUCHBASES
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ConnectionString{");
    sb.append("scheme=").append(scheme);
    sb.append(", user=").append(user(username));
    sb.append(", hosts=").append(system(hosts));
    sb.append(", params=").append(meta(params));
    sb.append('}');
    return sb.toString();
  }

  public static class UnresolvedSocket {
    private final String hostname;
    private final int port;

    UnresolvedSocket(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
    }

    public String hostname() {
      return hostname;
    }

    public int port() {
      return port;
    }

    @Override
    public String toString() {
      return "UnresolvedSocket{" +
          "hostname='" + system(hostname) + '\'' +
          ", port=" + system(port) +
          '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UnresolvedSocket that = (UnresolvedSocket) o;
      return port == that.port &&
          Objects.equals(hostname, that.hostname);
    }

    @Override
    public int hashCode() {
      return Objects.hash(hostname, port);
    }
  }
}
