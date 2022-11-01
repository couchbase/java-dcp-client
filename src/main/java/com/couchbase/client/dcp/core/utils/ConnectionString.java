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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.dcp.config.HostAndPort;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.client.dcp.core.utils.CbCollections.isNullOrEmpty;
import static com.couchbase.client.dcp.core.utils.CbCollections.listCopyOf;
import static com.couchbase.client.dcp.core.utils.CbCollections.transform;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Implements a {@link ConnectionString}.
 *
 * @author Michael Nitschinger
 * @since 2.4.0
 */
public class ConnectionString {

  public static final String DEFAULT_SCHEME = "couchbase://";

  private static final Pattern connectionStringPattern = Pattern.compile(
      "((?<scheme>.*?)://)?" +
          "((?<user>.*?)@)?" +
          "(?<hosts>.*?)" +
          "(\\?(?<params>.*?))?"
  );

  private final Scheme scheme;
  private final List<UnresolvedSocket> hosts;
  private final Map<String, String> params;
  private final @Nullable String username;

  private ConnectionString(Scheme scheme, @Nullable String username, List<UnresolvedSocket> hosts, Map<String, String> params) {
    this.scheme = requireNonNull(scheme);
    this.username = username;
    this.hosts = listCopyOf(hosts);
    this.params = unmodifiableMap(new LinkedHashMap<>(params));
  }

  protected ConnectionString(final String connectionString) {
    Matcher m = connectionStringPattern.matcher(connectionString);
    if (!m.matches()) {
      throw InvalidArgumentException.fromMessage("Malformed connection string: " + connectionString);
    }

    try {
      this.scheme = Optional.ofNullable(m.group("scheme"))
          .map(Scheme::parse)
          .orElse(Scheme.COUCHBASE);
      this.username = m.group("user");
      this.hosts = unmodifiableList(parseHosts(m.group("hosts")));
      this.params = unmodifiableMap(parseParams(m.group("params")));

    } catch (Exception e) {
      throw InvalidArgumentException.fromMessage("Failed to parse connection string \"" + connectionString + "\" ; " + e.getMessage(), e);
    }
  }

  public static ConnectionString create(final String connectionString) {
    return new ConnectionString(connectionString);
  }

  public static ConnectionString fromHostnames(final List<String> hostnames) {
    return create(String.join(",", hostnames));
  }

  @Stability.Internal
  public ConnectionString withScheme(Scheme scheme) {
    return new ConnectionString(scheme, username(), hosts(), params());
  }

  @Stability.Internal
  public ConnectionString withParams(Map<String, String> params) {
    return new ConnectionString(scheme(), username(), hosts(), params);
  }

  private static List<UnresolvedSocket> parseHosts(String hosts) {
    return Arrays.stream(hosts.split(","))
        .map(String::trim)
        .filter(it -> !it.isEmpty())
        .map(UnresolvedSocket::parse)
        .collect(toList());
  }

  private static Map<String, String> parseParams(@Nullable String paramsString) {
    Map<String, String> result = new LinkedHashMap<>();
    if (!isNullOrEmpty(paramsString)) {
      for (String entry : paramsString.split("&")) {
        String[] nameAndValue = entry.split("=", 2);
        String name = nameAndValue[0];
        String value = nameAndValue.length == 1 ? "" : nameAndValue[1];
        result.put(name, value);
      }
    }
    return result;
  }

  public Scheme scheme() {
    return scheme;
  }

  @Nullable
  public String username() {
    return username;
  }

  public List<UnresolvedSocket> hosts() {
    return hosts;
  }

  public Map<String, String> params() {
    return params;
  }

  /**
   * Returns true if this connection string consists of a single hostname (not IP address) with no port.
   */
  public boolean isValidDnsSrv() {
    // The core-io bits that check for an IP literal are not accessible from here.
    // For now, delegate to a core-io connection string.
    return com.couchbase.client.core.util.ConnectionString.create(this.original()).isValidDnsSrv();
  }

  /**
   * If this connection string consists of a single hostname (not IP address) with no port,
   * returns that hostname. Otherwise, returns empty.
   */
  public Optional<String> dnsSrvCandidate() {
    return isValidDnsSrv() ? Optional.of(hosts.get(0).hostname()) : Optional.empty();
  }

  public enum Scheme {
    COUCHBASE,
    COUCHBASES,
    ;

    private static Scheme parse(String s) {
      try {
        return valueOf(s.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        List<String> lowercaseNames = transform(values(), it -> it.name().toLowerCase(Locale.ROOT));
        throw InvalidArgumentException.fromMessage("Expected scheme to be one of " + lowercaseNames + " but got: " + s);
      }
    }
  }

  @Override
  public String toString() {
    return "ConnectionString{" +
        "scheme=" + scheme +
        ", user=" + username +
        ", hosts=" + hosts +
        ", params=" + params +
        '}';
  }

  /**
   * Returns this connection string formatted as a string.
   * <p>
   * The result can be passed to {@link #create(String)} to get the same connection string back again.
   */
  public String original() {
    StringBuilder sb = new StringBuilder();
    sb.append(scheme.name().toLowerCase(Locale.ROOT)).append("://");
    if (username != null) {
      sb.append(username).append("@");
    }
    sb.append(String.join(",", transform(hosts, UnresolvedSocket::format)));
    if (!params.isEmpty()) {
      sb.append("?");
      sb.append(String.join(
          "&",
          transform(params.entrySet(), it -> it.getKey() + "=" + it.getValue()))
      );
    }
    return sb.toString();
  }

  public static class UnresolvedSocket {
    private final HostAndPort hostAndPort;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<PortType> portType;

    UnresolvedSocket(HostAndPort hostAndPort, @Nullable PortType portType) {
      this.hostAndPort = requireNonNull(hostAndPort);
      this.portType = Optional.ofNullable(portType);
    }

    /**
     * @deprecated Please use {@link #host()} instead.
     */
    @Deprecated
    public String hostname() {
      return host();
    }

    public String host() {
      return hostAndPort.host();
    }

    public int port() {
      return hostAndPort.port();
    }

    public Optional<PortType> portType() {
      return portType;
    }

    public String format() {
      StringBuilder sb = new StringBuilder(hostAndPort.format());
      portType.ifPresent(it -> sb.append("=").append(it.name().toLowerCase(Locale.ROOT)));
      return sb.toString();
    }

    /**
     * @param address "host" or "host:port" or "host:port=portType"
     */
    static UnresolvedSocket parse(String address) {
      String[] parts = address.split("=", 2);
      HostAndPort hostAndPort = HostAndPort.parse(parts[0]);
      PortType portType = parts.length == 1 ? null : PortType.fromString(parts[1]);
      if (hostAndPort.port() == 0 && portType != null) {
        throw new IllegalArgumentException("Malformed address; must specify a port when specifying a port type: " + address);
      }
      return new UnresolvedSocket(hostAndPort, portType);
    }

    @Override
    public String toString() {
      return "UnresolvedSocket{" +
          "hostname='" + host() + '\'' +
          ", port=" + port() +
          ", portType=" + portType +
          '}';
    }
  }

  @Stability.Internal
  public enum PortType {
    MANAGER,
    KV;

    /**
     * Turn the raw representation into an enum.
     * <p>
     * Note that we support both "http" and "mcd" from libcouchbase to be compatible, but also expose "manager"
     * and "kv" so it more aligns with the current terminology of services.
     *
     * @param input the raw representation from the connstr.
     * @return the enum if it could be determined.
     */
    static PortType fromString(final String input) {
      if (input.equalsIgnoreCase("http") || input.equalsIgnoreCase("manager")) {
        return PortType.MANAGER;
      } else if (input.equalsIgnoreCase("mcd") || input.equalsIgnoreCase("kv")) {
        return PortType.KV;
      } else {
        throw InvalidArgumentException.fromMessage("Unsupported port type \"" + input + "\"");
      }
    }
  }

}
