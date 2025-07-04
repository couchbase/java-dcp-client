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

import com.couchbase.client.core.node.StandardMemcachedHashingStrategy;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.TopologyParser;
import com.couchbase.client.core.topology.TopologyRevision;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.buffer.DcpBucketConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.ReplayProcessor;

import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.redactSystem;
import static java.util.Objects.requireNonNull;

/**
 * Manages the bucket config stream.
 * <p>
 * Other components can listen to the stream by calling {@link #configs()}
 * or submit new configs by calling {@link #accept(HostAndPort, String)}.
 */
public class BucketConfigArbiter implements BucketConfigSink, BucketConfigSource {
  private static final Logger log = LoggerFactory.getLogger(BucketConfigArbiter.class);

  private final ReplayProcessor<DcpBucketConfig> configStream = ReplayProcessor.cacheLast();
  private final FluxSink<DcpBucketConfig> configSink = configStream.sink(FluxSink.OverflowStrategy.LATEST);

  private final Object revLock = new Object();

  // @GuardedBy("revLock")
  private TopologyRevision currentRev = TopologyRevision.ZERO;

  private final Client.Environment environment;

  public BucketConfigArbiter(Client.Environment environment) {
    this.environment = requireNonNull(environment);
  }

  @Override
  public void accept(HostAndPort origin, String rawConfig, TopologyRevision rev) {
    synchronized (revLock) {
      if (!rev.newerThan(currentRev)) {
        log.debug("Ignoring config revision {} from {}; not newer than current revision {}", rev, origin, currentRev);
        return;
      }

      log.debug("Received config revision {} from {} -> {}", rev, origin, redactSystem(rawConfig));

      if (!rawConfig.contains("\"nodeLocator\"")) {
        log.info("Received a global config (revision {})." +
                " Ignoring it, because a global config doesn't have info about the bucket." +
                " Waiting for a bucket config instead!",
            rev);
        return;
      }

      try {
        currentRev = rev;

        TopologyParser parser = new TopologyParser(
            environment.networkSelector(),
            environment.portSelector(),
            StandardMemcachedHashingStrategy.INSTANCE // doesn't matter for DCP, which doesn't support Memcached buckets
        );
        ClusterTopologyWithBucket config = parser.parse(rawConfig, origin.host()).requireBucket();

        configSink.next(new DcpBucketConfig(config));

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
  public Flux<DcpBucketConfig> configs() {
    return configStream;
  }

  private static final Pattern REV_PATTERN = Pattern.compile("\"rev\"\\s*:\\s*(-?\\d+)");
  private static final Pattern REV_EPOCH_PATTERN = Pattern.compile("\"revEpoch\"\\s*:\\s*(-?\\d+)");

  private static OptionalLong matchLong(Pattern pattern, String s) {
    Matcher m = pattern.matcher(s);
    return m.find() ? OptionalLong.of(Long.parseLong(m.group(1))) : OptionalLong.empty();
  }

  private static TopologyRevision getRev(String rawConfig) {
    long rev = matchLong(REV_PATTERN, rawConfig).orElseThrow(() ->
        new IllegalArgumentException("Failed to locate revision property in " + redactSystem(rawConfig)));

    long epoch = matchLong(REV_EPOCH_PATTERN, rawConfig).orElse(0);

    return new TopologyRevision(epoch, rev);
  }
}
