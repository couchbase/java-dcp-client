/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.buffer.DcpBucketConfig;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.config.NodeInfo;
import com.couchbase.client.dcp.core.state.LifecycleState;
import com.couchbase.client.dcp.core.state.NotConnectedException;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.events.FailedToAddNodeEvent;
import com.couchbase.client.dcp.events.FailedToMovePartitionEvent;
import com.couchbase.client.dcp.events.FailedToRemoveNodeEvent;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.KeyExtractor;
import com.couchbase.client.dcp.message.PartitionAndSeqno;
import com.couchbase.client.dcp.metrics.DcpClientMetrics;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class Conductor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Conductor.class);

  private final BucketConfigArbiter bucketConfigArbiter;
  private final Set<DcpChannel> channels = ConcurrentHashMap.newKeySet();
  private volatile boolean stopped = true;
  private final Client.Environment env;
  private final AtomicReference<DcpBucketConfig> currentConfig = new AtomicReference<>();
  private final SessionState sessionState = new SessionState();
  private final DcpClientMetrics metrics;

  // Serializes config updates so synchronization is not required in reconfigure()
  private final ExecutorService configUpdateExecutor = Executors.newSingleThreadExecutor(
      new DefaultThreadFactory("reconfigure", true));

  // Reaches zero when at least one configuration has been successfully applied.
  private final CountDownLatch configurationApplied = new CountDownLatch(1);

  public Conductor(final Client.Environment env, DcpClientMetrics metrics) {
    this.metrics = requireNonNull(metrics);
    this.env = env;
    this.bucketConfigArbiter = new BucketConfigArbiter(env);

    bucketConfigArbiter.configs()
        .publishOn(Schedulers.fromExecutor(configUpdateExecutor))
        .subscribe(config -> {
          // Couchbase sometimes says a newly created bucket has no partitions.
          // This doesn't affect cluster topology, but it's a problem for code
          // that needs to know the real partition count during startup.
          if (config.numberOfPartitions() == 0 && currentConfig.get() == null) {
            // Skip this config. The server will send another when the bucket is really ready.
            LOGGER.debug("Skipping initial config (rev {}) because it has invalid partition count.", config.rev());
            return;
          }

          LOGGER.trace("Applying new configuration, new rev is {}.", config.rev());
          currentConfig.set(config);
          reconfigure(config);
          configurationApplied.countDown();
        });
  }

  public SessionState sessionState() {
    return sessionState;
  }

  public Mono<Void> connect() {
    stopped = false;

    // Connect to every node listed in the bootstrap list.
    // As part of the connection process, each node is asked for the
    // bucket config. The response is used to reconfigure the cluster
    // which adds any missing nodes.
    env.clusterAt().forEach(this::add);
    updateChannelGauges();

    long bootstrapTimeoutMillis = env.bootstrapTimeout().toMillis()
        + env.configRefreshInterval().toMillis(); // allow at least one config refresh

    // Report completion when at least one configuration has been applied.
    return await(configurationApplied, bootstrapTimeoutMillis, TimeUnit.MILLISECONDS)
        .doOnError(throwable -> LOGGER.warn("Did not receive initial configuration from cluster within {}ms", bootstrapTimeoutMillis));
  }

  /**
   * Returns a completable that blocks until the latch count reaches zero.
   *
   * @throws RuntimeException (async) caused by TimeoutException if count does not reach zero before timeout.
   * @throws RuntimeException (async) caused by InterruptedException if interrupted while waiting.
   */
  private static Mono<Void> await(CountDownLatch latch, long timeout, TimeUnit timeoutUnit) {
    return Mono.fromRunnable(() -> {
      try {
        if (!latch.await(timeout, timeoutUnit)) {
          throw new RuntimeException(new TimeoutException("Timed out after waiting " + timeout + " " + timeoutUnit + " for latch."));
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  BucketConfigArbiter bucketConfigArbiter() {
    return bucketConfigArbiter;
  }

  /**
   * Returns true if all channels and the config provider are in a disconnected state.
   */
  public boolean disconnected() {
    return channels.stream()
        .allMatch(c -> c.isState(LifecycleState.DISCONNECTED));
  }

  public Mono<Void> stop() {
    LOGGER.debug("Instructed to shutdown.");
    stopped = true;
    return Flux.fromIterable(channels)
        .flatMap(DcpChannel::disconnect)
        .then(Mono.fromRunnable(configUpdateExecutor::shutdown))
        .then()
        .doOnSuccess(ignore -> LOGGER.info("Shutdown complete."));
  }

  /**
   * Returns the total number of partitions.
   */
  public int numberOfPartitions() {
    return currentConfig.get().numberOfPartitions();
  }

  public Flux<PartitionAndSeqno> getSeqnos() {
    return Flux.fromIterable(channels)
        .flatMap(this::getSeqnosForChannel)
        .flatMap(Flux::fromIterable);
  }

  private Mono<List<PartitionAndSeqno>> getSeqnosForChannel(final DcpChannel channel) {
    return Mono.just(channel)
        .flatMap(DcpChannel::getSeqnos)
        .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(200))
            .filter(e -> e instanceof NotConnectedException)
            .doAfterRetry(retrySignal -> LOGGER.debug("Rescheduling get Seqnos for channel {}, not connected (yet).", channel))
        );
  }

  public Mono<ByteBuf> getFailoverLog(final int partition) {
    return Mono.just(partition)
        .map(ignored -> activeChannelByPartition(partition))
        .flatMap(channel -> channel.getFailoverLog(partition))
        .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(200))
            .filter(e -> e instanceof NotConnectedException)
            .doAfterRetry(retrySignal -> LOGGER.debug("Rescheduling Get Failover Log for vbid {}, not connected (yet).", partition))
        );
  }

  public Mono<Void> startStreamForPartition(final int partition, final StreamOffset startOffset, final long endSeqno) {
    return Mono.just(partition)
        .map(this::activeChannelByPartition)
        .flatMap(channel -> channel.getCollectionsManifest()
            .flatMap(manifest -> {
              final CollectionsManifest m = manifest.orElse(CollectionsManifest.DEFAULT);
              final PartitionState ps = sessionState.get(partition);
              ps.setCollectionsManifest(m);
              ps.setKeyExtractor(manifest.isPresent() ? KeyExtractor.COLLECTIONS : KeyExtractor.NO_COLLECTIONS);
              ps.setMostRecentOpenStreamOffset(startOffset);
              return channel.openStream(partition, startOffset, endSeqno, m);
            })
        )
        .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(200))
            .filter(e -> e instanceof NotConnectedException)
            .doAfterRetry(retrySignal -> LOGGER.debug("Rescheduling Stream Start for vbid {}, not connected (yet).", partition))
        );
  }

  public Mono<Void> stopStreamForPartition(final int partition) {
    if (streamIsOpen(partition)) {
      DcpChannel channel = activeChannelByPartition(partition);
      return channel.closeStream(partition);
    } else {
      return Mono.empty();
    }
  }

  public boolean streamIsOpen(final int partition) {
    DcpChannel channel = activeChannelByPartition(partition);
    return channel.streamIsOpen(partition);
  }

  /**
   * Returns the dcp channel responsible for a given vbucket id according to the current
   * configuration.
   * <p>
   * Note that this doesn't mean that the partition is enabled there, it just checks the current
   * mapping.
   */
  private DcpChannel activeChannelByPartition(int partition) {
    final HostAndPort address = currentConfig.get().getActiveNodeKvAddress(partition);
    for (DcpChannel ch : channels) {
      if (ch.address().equals(address)) {
        return ch;
      }
    }

    throw new IllegalStateException("No DcpChannel found for partition " + partition);
  }

  private void reconfigure(DcpBucketConfig configHelper) {
    metrics.incrementReconfigure();

    final List<NodeInfo> nodes = configHelper.getDataNodes();
    if (nodes.isEmpty()) {
      throw new IllegalStateException("Bucket config helper returned no data nodes");
    }

    final Map<HostAndPort, DcpChannel> existingChannelsByAddress = channels.stream()
        .collect(toMap(DcpChannel::address, c -> c));

    final Set<HostAndPort> nodeAddresses = nodes.stream()
        .map(configHelper::getAddress)
        .collect(toSet());

    boolean nodesChanged = false;

    for (HostAndPort address : nodeAddresses) {
      if (!existingChannelsByAddress.containsKey(address)) {
        metrics.incrementAddChannel();
        add(address);
        nodesChanged = true;
      }
    }

    for (Map.Entry<HostAndPort, DcpChannel> entry : existingChannelsByAddress.entrySet()) {
      if (!nodeAddresses.contains(entry.getKey())) {
        metrics.incrementRemoveChannel();
        remove(entry.getValue());
        nodesChanged = true;
      }
    }

    // Don't (re)register the gauges unless something changed, because registration
    // requires first unregistering the old gauges, and that can cause unpleasant
    // unpleasant user experience when the metrics are exported via JMX
    // (the gauges temporarily vanish from the JMX browser).
    if (nodesChanged) {
      updateChannelGauges();
    }
  }

  private void updateChannelGauges() {
    metrics.registerConnectionStatusGauges(channels);
  }

  private void add(final HostAndPort node) {
    LOGGER.info("Adding DCP Channel against {}", system(node));
    final DcpChannel channel = new DcpChannel(node, env, this, metrics);
    if (!channels.add(channel)) {
      throw new IllegalStateException("Tried to add duplicate channel: " + system(channel));
    }

    channel.connect()
        .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(200))
            .filter(t -> !stopped)
            .doAfterRetry(retrySignal -> LOGGER.debug("Rescheduling Node reconnect for DCP channel {}", node))
        )
        .doOnSuccess(ignored -> LOGGER.debug("Completed Node connect for DCP channel {}", node))
        .onErrorResume(e -> {
          LOGGER.warn("Got error during connect (maybe retried) for node {}", system(node), e);
          if (env.eventBus() != null) {
            env.eventBus().publish(new FailedToAddNodeEvent(node, e));
          }
          return Mono.empty();
        }).subscribe();
  }

  private void remove(final DcpChannel node) {
    if (!channels.remove(node)) {
      throw new IllegalStateException("Tried to remove unknown channel: " + system(node));
    }

    LOGGER.info("Removing DCP Channel against {}", system(node));

    for (int partition = 0; partition < node.streamIsOpen.length(); partition++) {
      if (node.streamIsOpen(partition)) {
        maybeMovePartition(partition);
      }
    }

    node.disconnect()
        .doOnSuccess(ignored -> LOGGER.debug("Channel remove notified as complete for {}", node.address()))
        .onErrorResume(e -> {
          LOGGER.warn("Got error during Node removal for node {}", system(node.address()), e);
          if (env.eventBus() != null) {
            env.eventBus().publish(new FailedToRemoveNodeEvent(node.address(), e));
          }
          return Mono.empty();
        }).subscribe();
  }

  /**
   * Called by the {@link DcpChannel} to signal a stream end done by the server and it
   * most likely needs to be moved over to a new node during rebalance/failover.
   *
   * @param partition the partition to move if needed
   */
  void maybeMovePartition(final int partition) {
    Mono.just(partition)
        .delayElement(Duration.ofMillis(50))
        .filter(ignored -> {
          PartitionState ps = sessionState.get(partition);
          boolean desiredSeqnoReached = ps.isAtEnd();
          if (desiredSeqnoReached) {
            LOGGER.debug("Reached desired high seqno {} for vbucket {}, not reopening stream.",
                ps.getEndSeqno(), partition);
          }
          return !desiredSeqnoReached;
        })
        .flatMap(ignored -> {
          PartitionState ps = sessionState.get(partition);
          return startStreamForPartition(
              partition,
              ps.getOffset(),
              ps.getEndSeqno()
          ).retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(200))
              .filter(e -> e instanceof NotMyVbucketException));
        })

        .doOnSubscribe(subscription -> LOGGER.debug("Subscribing for Partition Move for partition {}", partition))
        .doOnSuccess(ignored -> LOGGER.trace("Completed Partition Move for partition {}", partition))
        .onErrorResume(e -> {
          if (e instanceof RollbackException) {
            // A synthetic "rollback" message has already been passed to the to the Control Event Handler,
            // which may react by calling Client.rollbackAndRestartStream().
            //
            // Don't log a scary stack trace, and don't publish an event that would cause
            // EventHandlerAdapter to signal a stream failure.
            LOGGER.warn("Rollback during Partition Move for partition {}", partition);
          } else {
            LOGGER.warn("Error during Partition Move for partition {}", partition, e);
            if (env.eventBus() != null) {
              env.eventBus().publish(new FailedToMovePartitionEvent(partition, e));
            }
          }
          return Mono.empty();
        })
        .subscribe();
  }
}
