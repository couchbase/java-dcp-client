/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.core.config;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.dcp.core.env.NetworkResolution;
import com.couchbase.client.dcp.core.service.ServiceType;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;


public class NodeInfoParser {
  private NodeInfoParser() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Parses one element of the "nodesExt" array. Returns a map where the keys are the
   * external address network names (plus the implicit "default" network),
   * and each value is the candidate NodeInfo object for the associated network.
   *
   * @param portSelector Determines whether the returned node info has TLS ports or non-TLS ports.
   */
  public static Map<NetworkResolution, NodeInfo> parse(ObjectNode json, PortSelector portSelector) {
    Map<NetworkResolution, NodeInfo> result = new HashMap<>();

    NodeInfo defaultConfig = parseOne(json, "services", portSelector);
    result.put(NetworkResolution.DEFAULT, defaultConfig);

    ObjectNode alternatesNode = (ObjectNode) json.get("alternateAddresses");
    if (alternatesNode == null) {
      return result;
    }

    alternatesNode.fields().forEachRemaining(it -> {
      NetworkResolution network = NetworkResolution.valueOf(it.getKey());
      NodeInfo alternate = parseOne((ObjectNode) it.getValue(), "ports", portSelector);

      // The server may omit the host or ports from the alternate address entry
      // if they are the same as on the default network, or so I'm told...
      alternate = alternate.withMissingInfoFrom(defaultConfig);

      result.put(network, alternate);
    });

    return result;
  }

  /**
   * @param portsFieldName because of course it's different when parsing alternate addresses :-/
   */
  private static NodeInfo parseOne(ObjectNode json, String portsFieldName, PortSelector portSelector) {
    String host = json.path("hostname").textValue();

    // Apparently, ancient versions of Couchbase (like, 3.x) could omit the hostname field,
    // and the client would have to get it from the `nodes` list.
    // With Couchbase 5.0, that doesn't happen anymore. Sanity check, just in case:
    if (host == null) {
      throw new ConfigException("Couchbase server version is too old for this SDK; nodesExt entry is missing 'hostname' field.");
    }

    return new NodeInfo(
        host,
        parseServices((ObjectNode) json.get(portsFieldName), portSelector)
    );
  }

  private static Map<ServiceType, Integer> parseServices(@Nullable ObjectNode servicesNode, PortSelector portSelector) {
    if (servicesNode == null) {
      return emptyMap();
    }

    Map<ServiceType, Integer> serviceToPort = new HashMap<>();
    servicesNode.fields().forEachRemaining(serviceField -> {
      String serviceName = serviceField.getKey();
      portSelector.getServiceForName(serviceName).ifPresent(serviceType -> {
        JsonNode portNode = serviceField.getValue();
        serviceToPort.put(serviceType, portNode.intValue());
      });
    });
    return serviceToPort;
  }

}
