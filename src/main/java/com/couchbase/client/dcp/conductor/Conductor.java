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

import com.couchbase.client.dcp.buffer.DcpBucketConfig;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.config.NodeInfo;
import com.couchbase.client.dcp.core.state.LifecycleState;
import com.couchbase.client.dcp.core.state.NotConnectedException;
import com.couchbase.client.dcp.core.time.Delay;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.events.FailedToAddNodeEvent;
import com.couchbase.client.dcp.events.FailedToMovePartitionEvent;
import com.couchbase.client.dcp.events.FailedToRemoveNodeEvent;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.KeyExtractor;
import com.couchbase.client.dcp.metrics.DcpClientMetrics;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.util.retry.RetryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.CompletableSubscriber;
import rx.Observable;
import rx.Single;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.util.retry.RetryBuilder.anyOf;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class Conductor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Conductor.class);

  private final BucketConfigArbiter bucketConfigArbiter;
  private final Set<DcpChannel> channels = ConcurrentHashMap.newKeySet();
  private volatile boolean stopped = true;
  private final ClientEnvironment env;
  private final AtomicReference<DcpBucketConfig> currentConfig = new AtomicReference<>();
  private final SessionState sessionState = new SessionState();
  private final DcpClientMetrics metrics;

  // Serializes config updates so synchronization is not required in reconfigure()
  private final ExecutorService configUpdateExecutor = Executors.newSingleThreadExecutor(
      new DefaultThreadFactory("reconfigure", true));

  // Reaches zero when at least one configuration has been successfully applied.
  private final CountDownLatch configurationApplied = new CountDownLatch(1);

  public Conductor(final ClientEnvironment env, DcpClientMetrics metrics) {
    this.metrics = requireNonNull(metrics);
    this.env = env;
    this.bucketConfigArbiter = new BucketConfigArbiter(env);

    bucketConfigArbiter.configs()
        .observeOn(Schedulers.from(configUpdateExecutor))
        .forEach(config -> {
          LOGGER.trace("Applying new configuration, new rev is {}.", config.rev());
          currentConfig.set(config);
          reconfigure(config);
          configurationApplied.countDown();
        });
  }

  public SessionState sessionState() {
    return sessionState;
  }

  public Completable connect() {
    stopped = false;

    // Connect to every node listed in the bootstrap list.
    // As part of the connection process, each node is asked for the
    // bucket config. The response is used to reconfigure the cluster
    // which adds any missing nodes.
    env.clusterAt().forEach(this::add);

    long bootstrapTimeoutMillis = env.bootstrapTimeout().toMillis()
        + env.configRefreshInterval().toMillis(); // allow at least one config refresh

    // Report completion when the partition count is accurate and
    // at least one configuration has been applied.
    return bucketConfigArbiter().configs()
        // Couchbase sometimes says a newly created bucket has no partitions.
        // This doesn't affect cluster topology, but it's a problem for users
        // who need the real partition count. Wait for a "real" config before proceeding.
        .filter(config -> config.numberOfPartitions() != 0)
        .first().toCompletable()

        // We're about to block on a latch, so switch to a thread that doesn't mind.
        .observeOn(Schedulers.io())
        .andThen(await(configurationApplied, bootstrapTimeoutMillis, TimeUnit.MILLISECONDS))

        // All of this should complete within the bootstrap timeout.
        .timeout(bootstrapTimeoutMillis, TimeUnit.MILLISECONDS)
        .doOnError(throwable -> LOGGER.warn("Did not receive initial configuration from cluster within {}ms", bootstrapTimeoutMillis));
  }

  /**
   * Returns a completable that blocks until the count down reaches zero.
   */
  private static Completable await(CountDownLatch latch, long timeout, TimeUnit timeoutUnit) {
    return Completable.fromAction(() -> {
      try {
        latch.await(timeout, timeoutUnit);
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

  public Completable stop() {
    LOGGER.debug("Instructed to shutdown.");
    stopped = true;
    Completable channelShutdown = Observable
        .from(channels)
        .flatMapCompletable(DcpChannel::disconnect)
        .toCompletable()
        .andThen(Completable.fromAction(configUpdateExecutor::shutdown));

    return channelShutdown.doOnCompleted(() -> LOGGER.info("Shutdown complete."));
  }

  /**
   * Returns the total number of partitions.
   */
  public int numberOfPartitions() {
    return currentConfig.get().numberOfPartitions();
  }

  public Observable<ByteBuf> getSeqnos() {
    return Observable
        .from(channels)
        .flatMap(this::getSeqnosForChannel);
  }

  private Observable<ByteBuf> getSeqnosForChannel(final DcpChannel channel) {
    return Observable
        .just(channel)
        .flatMapSingle(DcpChannel::getSeqnos)
        .retryWhen(anyOf(NotConnectedException.class)
            .max(Integer.MAX_VALUE)
            .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
            .doOnRetry((retry, cause, delay, delayUnit) -> LOGGER.debug("Rescheduling get Seqnos for channel {}, not connected (yet).", channel))
            .build()
        );
  }

  public Single<ByteBuf> getFailoverLog(final short partition) {
    return Observable
        .just(partition)
        .map(ignored -> activeChannelByPartition(partition))
        .flatMapSingle(channel -> channel.getFailoverLog(partition))
        .retryWhen(anyOf(NotConnectedException.class)
            .max(Integer.MAX_VALUE)
            .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
            .doOnRetry((retry, cause, delay, delayUnit) -> LOGGER.debug("Rescheduling Get Failover Log for vbid {}, not connected (yet).", partition))
            .build()
        ).toSingle();
  }

  public Completable startStreamForPartition(final short partition, final StreamOffset startOffset, final long endSeqno) {
    return Observable
        .just(partition)
        .map(ignored -> activeChannelByPartition(partition))
        .flatMapCompletable(channel ->
            channel.getCollectionsManifest()
                .flatMapCompletable(manifest -> {
                  final CollectionsManifest m = manifest.orElse(CollectionsManifest.DEFAULT);
                  final PartitionState ps = sessionState.get(partition);
                  ps.setCollectionsManifest(m);
                  ps.setKeyExtractor(manifest.isPresent() ? KeyExtractor.COLLECTIONS : KeyExtractor.NO_COLLECTIONS);
                  return channel.openStream(partition, startOffset, endSeqno, m);
                })
        )
        .retryWhen(anyOf(NotConnectedException.class)
            .max(Integer.MAX_VALUE)
            .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
            .doOnRetry((retry, cause, delay, delayUnit) -> LOGGER.debug("Rescheduling Stream Start for vbid {}, not connected (yet).", partition))
            .build()
        )
        .toCompletable();
  }

  public Completable stopStreamForPartition(final short partition) {
    if (streamIsOpen(partition)) {
      DcpChannel channel = activeChannelByPartition(partition);
      return channel.closeStream(partition);
    } else {
      return Completable.complete();
    }
  }

  public boolean streamIsOpen(final short partition) {
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
  private DcpChannel activeChannelByPartition(short partition) {
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

    for (HostAndPort address : nodeAddresses) {
      if (!existingChannelsByAddress.containsKey(address)) {
        metrics.incrementAddChannel();
        add(address);
      }
    }

    for (Map.Entry<HostAndPort, DcpChannel> entry : existingChannelsByAddress.entrySet()) {
      if (!nodeAddresses.contains(entry.getKey())) {
        metrics.incrementRemoveChannel();
        remove(entry.getValue());
      }
    }
  }

  private void add(final HostAndPort node) {
    LOGGER.info("Adding DCP Channel against {}", system(node));
    final DcpChannel channel = new DcpChannel(node, env, this);
    if (!channels.add(channel)) {
      throw new IllegalStateException("Tried to add duplicate channel: " + system(channel));
    }

    channel
        .connect()
        .retryWhen(RetryBuilder.anyMatches(t -> !stopped)
            .max(env.dcpChannelsReconnectMaxAttempts())
            .delay(env.dcpChannelsReconnectDelay())
            .doOnRetry((retry, cause, delay, delayUnit) -> LOGGER.debug("Rescheduling Node reconnect for DCP channel {}", node))
            .build())
        .subscribe(new CompletableSubscriber() {
          @Override
          public void onCompleted() {
            LOGGER.debug("Completed Node connect for DCP channel {}", node);
          }

          @Override
          public void onError(Throwable e) {
            LOGGER.warn("Got error during connect (maybe retried) for node {}", system(node), e);
            if (env.eventBus() != null) {
              env.eventBus().publish(new FailedToAddNodeEvent(node, e));
            }
          }

          @Override
          public void onSubscribe(Subscription d) {
            // ignored.
          }
        });
  }

  private void remove(final DcpChannel node) {
    if (!channels.remove(node)) {
      throw new IllegalStateException("Tried to remove unknown channel: " + system(node));
    }

    LOGGER.info("Removing DCP Channel against {}", system(node));

    for (short partition = 0; partition < node.streamIsOpen.length(); partition++) {
      if (node.streamIsOpen(partition)) {
        maybeMovePartition(partition);
      }
    }

    node.disconnect().subscribe(new CompletableSubscriber() {
      @Override
      public void onCompleted() {
        LOGGER.debug("Channel remove notified as complete for {}", node.address());
      }

      @Override
      public void onError(Throwable e) {
        LOGGER.warn("Got error during Node removal for node {}", system(node.address()), e);
        if (env.eventBus() != null) {
          env.eventBus().publish(new FailedToRemoveNodeEvent(node.address(), e));
        }
      }

      @Override
      public void onSubscribe(Subscription d) {
        // ignored.
      }
    });
  }

  /**
   * Called by the {@link DcpChannel} to signal a stream end done by the server and it
   * most likely needs to be moved over to a new node during rebalance/failover.
   *
   * @param partition the partition to move if needed
   */
  void maybeMovePartition(final short partition) {
    Observable
        .timer(50, TimeUnit.MILLISECONDS)
        .filter(ignored -> {
          PartitionState ps = sessionState.get(partition);
          boolean desiredSeqnoReached = ps.isAtEnd();
          if (desiredSeqnoReached) {
            LOGGER.debug("Reached desired high seqno {} for vbucket {}, not reopening stream.",
                ps.getEndSeqno(), partition);
          }
          return !desiredSeqnoReached;
        })
        .flatMapCompletable(ignored -> {
          PartitionState ps = sessionState.get(partition);
          return startStreamForPartition(
              partition,
              ps.getOffset(),
              ps.getEndSeqno()
          ).retryWhen(anyOf(NotMyVbucketException.class)
              .max(Integer.MAX_VALUE)
              .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
              .build());
        }).toCompletable().subscribe(new CompletableSubscriber() {
      @Override
      public void onCompleted() {
        LOGGER.trace("Completed Partition Move for partition {}", partition);
      }

      @Override
      public void onError(Throwable e) {
        if (e instanceof RollbackException) {
          // Benign, so don't log a scary stack trace. A synthetic "rollback" message has been passed
          // to the Control Event Handler, which may react by calling Client.rollbackAndRestartStream().
          LOGGER.warn("Rollback during Partition Move for partition {}", partition);
        } else {
          LOGGER.warn("Error during Partition Move for partition {}", partition, e);
        }
        if (env.eventBus() != null) {
          env.eventBus().publish(new FailedToMovePartitionEvent(partition, e));
        }
      }

      @Override
      public void onSubscribe(Subscription d) {
        LOGGER.debug("Subscribing for Partition Move for partition {}", partition);
      }
    });
  }

}
