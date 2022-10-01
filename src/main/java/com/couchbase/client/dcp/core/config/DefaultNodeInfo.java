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
package com.couchbase.client.dcp.core.config;

import com.couchbase.client.dcp.core.CouchbaseException;
import com.couchbase.client.dcp.core.service.ServiceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation of {@link NodeInfo}.
 */
public class DefaultNodeInfo implements NodeInfo {

  private final String hostname;
  private final Map<ServiceType, Integer> directServices;
  private final Map<ServiceType, Integer> sslServices;
  private final Map<String, AlternateAddress> alternateAddresses;
  private int configPort;
  private volatile String useAlternateNetwork;


  /**
   * Creates a new {@link DefaultNodeInfo} with no SSL services.
   *
   * @param viewUri  the URI of the view service.
   * @param hostname the hostname of the node.
   * @param ports    the port list of the node services.
   */
  @JsonCreator
  public DefaultNodeInfo(
      @JsonProperty("couchApiBase") String viewUri,
      @JsonProperty("hostname") String hostname,
      @JsonProperty("ports") Map<String, Integer> ports,
      @JsonProperty("alternateAddresses") Map<String, AlternateAddress> alternateAddresses) {
    if (hostname == null) {
      throw new CouchbaseException(new IllegalArgumentException("NodeInfo hostname cannot be null"));
    }
    this.alternateAddresses = alternateAddresses == null
        ? Collections.<String, AlternateAddress>emptyMap()
        : alternateAddresses;

    this.hostname = trimPort(hostname);
    this.directServices = parseDirectServices(viewUri, ports);
    this.sslServices = new HashMap<>();
  }

  /**
   * Creates a new {@link DefaultNodeInfo} with SSL services.
   *
   * @param hostname the hostname of the node.
   * @param direct   the port list of the direct node services.
   * @param ssl      the port list of the ssl node services.
   */
  public DefaultNodeInfo(String hostname, Map<ServiceType, Integer> direct,
                         Map<ServiceType, Integer> ssl, Map<String, AlternateAddress> alternateAddresses) {
    if (hostname == null) {
      throw new CouchbaseException(new IllegalArgumentException("NodeInfo hostname cannot be null"));
    }

    this.hostname = hostname;
    this.directServices = direct;
    this.sslServices = ssl;
    this.alternateAddresses = alternateAddresses == null
        ? Collections.<String, AlternateAddress>emptyMap()
        : alternateAddresses;
  }

  @Override
  public String hostname() {
    return hostname;
  }

  @Override
  public Map<ServiceType, Integer> services() {
    return directServices;
  }

  @Override
  public Map<ServiceType, Integer> sslServices() {
    return sslServices;
  }

  @Override
  public Map<String, AlternateAddress> alternateAddresses() {
    return alternateAddresses;
  }

  private Map<ServiceType, Integer> parseDirectServices(final String viewUri, final Map<String, Integer> input) {
    Map<ServiceType, Integer> services = new HashMap<>();
    for (Map.Entry<String, Integer> entry : input.entrySet()) {
      String type = entry.getKey();
      Integer port = entry.getValue();
      if (type.equals("direct")) {
        services.put(ServiceType.KV, port);
      }
    }
    services.put(ServiceType.MANAGER, configPort);
    if (viewUri != null) {
      try {
        services.put(ServiceType.VIEWS, new URL(viewUri).getPort());
      } catch (MalformedURLException ex) {
        throw new ConfigurationException("Could not parse VIEW URL.", ex);
      }
    }
    return services;
  }

  private String trimPort(final String hostname) {
    String[] parts = hostname.split(":");
    configPort = Integer.parseInt(parts[parts.length - 1]);

    if (parts.length > 2) {
      // Handle IPv6 syntax
      String assembledHost = "";
      for (int i = 0; i < parts.length - 1; i++) {
        assembledHost += parts[i];
        if (parts[i].endsWith("]")) {
          break;
        } else {
          assembledHost += ":";
        }
      }
      if (assembledHost.startsWith("[") && assembledHost.endsWith("]")) {
        return assembledHost.substring(1, assembledHost.length() - 1);
      }
      return assembledHost;
    } else {
      // Simple IPv4 Handling
      return parts[0];
    }
  }

  @Override
  public String useAlternateNetwork() {
    return useAlternateNetwork;
  }

  @Override
  public void useAlternateNetwork(final String useAlternateNetwork) {
    this.useAlternateNetwork = useAlternateNetwork;
  }

  @Override
  public String toString() {
    return "DefaultNodeInfo{" +
        ", hostname=" + hostname +
        ", directServices=" + directServices +
        ", sslServices=" + sslServices +
        ", alternateAddresses=" + alternateAddresses +
        ", configPort=" + configPort +
        ", useAlternateNetwork=" + useAlternateNetwork +
        '}';
  }
}
