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

import com.couchbase.client.dcp.core.service.ServiceType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Helps the config parser select only the service ports relevant
 * to the client's transport security mode (TLS or non-TLS).
 */
public enum PortSelector {
  TLS(ServiceIndexes.tlsServiceIndex),
  NON_TLS(ServiceIndexes.nonTlsServiceIndex),
  ;

  /**
   * Key is the name of a service as it appears in the nodesExt section of config JSON,
   * for example: "mgmt" or "mgmtSSL". Value is the service enum value associated with the name.
   */
  private final Map<String, ServiceType> serviceIndex;

  PortSelector(Map<String, ServiceType> serviceIndex) {
    this.serviceIndex = requireNonNull(serviceIndex);
  }

  /**
   * Given the name of a Couchbase service as it appears in the global/bucket config JSON,
   * return the associated ServiceType, or empty if the name is unrecognized or refers to
   * an irrelevant transport. For example, {@link PortSelector#TLS} only recognizes service names
   * that use a secure transport.
   */
  public Optional<ServiceType> getServiceForName(String serviceName) {
    return Optional.ofNullable(serviceIndex.get(serviceName));
  }

  private static class ServiceIndexes {
    // Map from service name (as it appears in the JSON from the server) to service type.
    private static final Map<String, ServiceType> tlsServiceIndex = new HashMap<>();
    private static final Map<String, ServiceType> nonTlsServiceIndex = new HashMap<>();

    private static void registerService(ServiceType service, String nonTlsServiceName, String tlsServiceName) {
      tlsServiceIndex.put(tlsServiceName, service);
      nonTlsServiceIndex.put(nonTlsServiceName, service);
    }

    static {
      registerService(ServiceType.MANAGER, "mgmt", "mgmtSSL");
      registerService(ServiceType.KV, "kv", "kvSSL");
      registerService(ServiceType.VIEWS, "capi", "capiSSL");
      registerService(ServiceType.QUERY, "n1ql", "n1qlSSL");
      registerService(ServiceType.SEARCH, "fts", "ftsSSL");
      registerService(ServiceType.ANALYTICS, "cbas", "cbasSSL");
      registerService(ServiceType.EVENTING, "eventingAdminPort", "eventingSSL");
      registerService(ServiceType.BACKUP, "backupAPI", "backupAPIHTTPS");

      // Remind developer to register any new service types
      Set<ServiceType> unregistered = new HashSet<>(Arrays.asList(ServiceType.values()));
      unregistered.removeAll(tlsServiceIndex.values());
      if (!unregistered.isEmpty()) {
        throw new AssertionError("Missing registration for service type(s): " + unregistered);
      }
    }
  }
}
