/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp.conductor;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.buffer.DcpBucketConfig;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.config.AlternateAddress;
import com.couchbase.client.dcp.core.config.BucketConfig;
import com.couchbase.client.dcp.core.config.CouchbaseBucketConfig;
import com.couchbase.client.dcp.core.config.NodeInfo;
import com.couchbase.client.dcp.core.config.parser.BucketConfigParser;
import com.couchbase.client.dcp.core.env.NetworkResolution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static java.util.Objects.requireNonNull;

/**
 * Manages the bucket config stream.
 * <p>
 * Other components can listen to the stream by calling {@link #configs()}
 * or submit new configs by calling {@link #accept(HostAndPort, String)}.
 */
public class BucketConfigArbiter implements BucketConfigSink, BucketConfigSource {
  private static final Logger log = LoggerFactory.getLogger(BucketConfigArbiter.class);

  private final Subject<DcpBucketConfig, DcpBucketConfig> configStream = BehaviorSubject.<DcpBucketConfig>create().toSerialized();

  private final Object revLock = new Object();

  // @GuardedBy("revLock")
  private long currentRev = -1;

  // @GuardedBy("revLock")
  private boolean hasDeterminedAlternateNetworkName = false;

  // @GuardedBy("revLock")
  private String alternateNetworkName; // null means use primary

  private final Client.Environment environment;

  public BucketConfigArbiter(Client.Environment environment) {
    this.environment = requireNonNull(environment);
  }

  @Override
  public void accept(HostAndPort origin, String rawConfig, long rev) {
    synchronized (revLock) {
      if (rev <= currentRev) {
        log.debug("Ignoring bucket config revision {} from {}; not newer than current revision {}", origin, rev, currentRev);
        return;
      }

      log.debug("Received bucket config revision {} from {} -> {}", rev, origin, system(rawConfig));

      try {
        currentRev = rev;

        CouchbaseBucketConfig config = (CouchbaseBucketConfig) BucketConfigParser.parse(rawConfig, origin);
        selectAlternateNetwork(config);

        configStream.onNext(new DcpBucketConfig(config, environment.sslEnabled()));

      } catch (Exception e) {
        log.error("Failed to parse bucket config", e);
      }
    }
  }

  @Override
  public void accept(HostAndPort origin, String rawConfig) {
    try {
      accept(origin, rawConfig, getRev(rawConfig));
    } catch (Exception e) {
      log.error("Failed to parse bucket config", e);
    }
  }

  @Override
  public Observable<DcpBucketConfig> configs() {
    return configStream;
  }

  private void selectAlternateNetwork(CouchbaseBucketConfig config) {
    if (!Thread.holdsLock(revLock)) {
      throw new IllegalStateException("Must hold revLock");
    }

    if (!hasDeterminedAlternateNetworkName) {
      final Set<String> seedHosts = environment.clusterAt().stream()
          .map(HostAndPort::host)
          .collect(Collectors.toSet());
      alternateNetworkName = determineNetworkResolution(config, environment.networkResolution(), seedHosts);
      hasDeterminedAlternateNetworkName = true;

      String displayName = alternateNetworkName == null ? "<default>" : alternateNetworkName;
      if (NetworkResolution.AUTO.equals(environment.networkResolution())) {
        displayName = "auto -> " + displayName;
      }
      log.info("Selected network: {}", displayName);
    }
    config.useAlternateNetwork(alternateNetworkName);
  }

  /**
   * Helper method to figure out which network resolution should be used.
   * <p>
   * if DEFAULT is selected, then null is returned which is equal to the "internal" or default
   * config mode. If AUTO is used then we perform the select heuristic based off of the seed
   * hosts given. All other resolution settings (i.e. EXTERNAL) are returned directly and are
   * considered to be part of the alternate address configs.
   *
   * @param config the config to check against
   * @param nr the network resolution setting from the environment
   * @param seedHosts the seed hosts from bootstrap for autoconfig.
   * @return the found setting if external is used, null if internal/default is used.
   */
  private static String determineNetworkResolution(final BucketConfig config, final NetworkResolution nr,
                                                   final Set<String> seedHosts) {
    if (nr.equals(NetworkResolution.DEFAULT)) {
      return null;
    } else if (nr.equals(NetworkResolution.AUTO)) {
      for (NodeInfo info : config.nodes()) {
        if (seedHosts.contains(info.hostname())) {
          return null;
        }

        Map<String, AlternateAddress> aa = info.alternateAddresses();
        if (aa != null && !aa.isEmpty()) {
          for (Map.Entry<String, AlternateAddress> entry : aa.entrySet()) {
            AlternateAddress alternateAddress = entry.getValue();
            if (alternateAddress != null && seedHosts.contains(alternateAddress.hostname())) {
              return entry.getKey();
            }
          }
        }
      }
      return null;
    } else {
      return nr.name();
    }
  }

  private static final Pattern REV_PATTERN = Pattern.compile("\"rev\"\\s*:\\s*(-?\\d+)");

  private static long getRev(String rawConfig) {
    Matcher m = REV_PATTERN.matcher(rawConfig);
    if (m.find()) {
      return Long.parseLong(m.group(1));
    }
    throw new IllegalArgumentException("Failed to locate revision property in " + system(rawConfig));
  }
}
