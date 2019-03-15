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

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.buffer.BucketConfigHelper;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.events.FailedToAddNodeEvent;
import com.couchbase.client.dcp.events.FailedToMovePartitionEvent;
import com.couchbase.client.dcp.events.FailedToRemoveNodeEvent;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.util.retry.RetryBuilder;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.internal.ConcurrentSet;
import rx.Completable;
import rx.CompletableSubscriber;
import rx.Observable;
import rx.Single;
import rx.Subscription;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.util.retry.RetryBuilder.anyOf;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class Conductor {

  private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

  private final ConfigProvider configProvider;
  private final Set<DcpChannel> channels = new ConcurrentSet<>();
  private volatile boolean stopped = true;
  private final ClientEnvironment env;
  private final AtomicReference<CouchbaseBucketConfig> currentConfig = new AtomicReference<>();
  private final boolean ownsConfigProvider;
  private final SessionState sessionState = new SessionState();

  public Conductor(final ClientEnvironment env, ConfigProvider cp) {
    this.env = env;
    configProvider = cp == null ? new HttpStreamingConfigProvider(env) : cp;
    ownsConfigProvider = cp == null;
    configProvider.configs().forEach(config -> {
      LOGGER.trace("Applying new configuration, new rev is {}.", config.rev());
      currentConfig.set(config);
      reconfigure(config);
    });
  }

  public SessionState sessionState() {
    return sessionState;
  }

  public Completable connect() {
    stopped = false;
    Completable atLeastOneConfig = configProvider.configs()
        .filter(config -> config.numberOfPartitions() != 0)
        .first().toCompletable()
        .timeout(env.bootstrapTimeout(), TimeUnit.SECONDS)
        .doOnError(throwable -> LOGGER.warn("Did not receive initial configuration from provider."));
    return configProvider.start()
        .timeout(env.connectTimeout(), TimeUnit.SECONDS)
        .doOnError(throwable -> LOGGER.warn("Cannot connect configuration provider."))
        .concatWith(atLeastOneConfig);
  }

  ConfigProvider configProvider() {
    return configProvider;
  }

  /**
   * Returns true if all channels and the config provider are in a disconnected state.
   */
  public boolean disconnected() {
    if (!configProvider.isState(LifecycleState.DISCONNECTED)) {
      return false;
    }

    for (DcpChannel channel : channels) {
      if (!channel.isState(LifecycleState.DISCONNECTED)) {
        return false;
      }
    }

    return true;
  }

  public Completable stop() {
    LOGGER.debug("Instructed to shutdown.");
    stopped = true;
    Completable channelShutdown = Observable
        .from(channels)
        .flatMapCompletable(DcpChannel::disconnect)
        .toCompletable();

    if (ownsConfigProvider) {
      channelShutdown = channelShutdown.andThen(configProvider.stop());
    }

    return channelShutdown.doOnCompleted(() -> LOGGER.info("Shutdown complete."));
  }

  /**
   * Returns the total number of partitions.
   */
  public int numberOfPartitions() {
    CouchbaseBucketConfig config = currentConfig.get();
    return config.numberOfPartitions();
  }

  public Observable<ByteBuf> getSeqnos() {
    return Observable
        .from(channels)
        .flatMap(this::getSeqnosForChannel);
  }

  @SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
  public Single<ByteBuf> getFailoverLog(final short partition) {
    return Observable
        .just(partition)
        .map(ignored -> masterChannelByPartition(partition))
        .flatMapSingle(channel -> channel.getFailoverLog(partition))
        .retryWhen(anyOf(NotConnectedException.class)
            .max(Integer.MAX_VALUE)
            .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
            .doOnRetry((retry, cause, delay, delayUnit) -> LOGGER.debug("Rescheduling Get Failover Log for vbid {}, not connected (yet).", partition))
            .build()
        ).toSingle();
  }

  @SuppressWarnings("unchecked")
  public Completable startStreamForPartition(final short partition, final long vbuuid, final long startSeqno,
                                             final long endSeqno, final long snapshotStartSeqno, final long snapshotEndSeqno) {
    return Observable
        .just(partition)
        .map(ignored -> masterChannelByPartition(partition))
        .flatMapCompletable(channel -> channel.openStream(partition, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno))
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
      DcpChannel channel = masterChannelByPartition(partition);
      return channel.closeStream(partition);
    } else {
      return Completable.complete();
    }
  }

  public boolean streamIsOpen(final short partition) {
    DcpChannel channel = masterChannelByPartition(partition);
    return channel.streamIsOpen(partition);
  }

  /**
   * Returns the dcp channel responsible for a given vbucket id according to the current
   * configuration.
   * <p>
   * Note that this doesn't mean that the partition is enabled there, it just checks the current
   * mapping.
   */
  private DcpChannel masterChannelByPartition(short partition) {
    CouchbaseBucketConfig config = currentConfig.get();
    int index = config.nodeIndexForMaster(partition, false);
    NodeInfo node = config.nodeAtIndex(index);
    int port = (env.sslEnabled() ? node.sslServices() : node.services()).get(ServiceType.BINARY);
    InetSocketAddress address = new InetSocketAddress(node.hostname().nameOrAddress(), port);
    for (DcpChannel ch : channels) {
      if (ch.address().equals(address)) {
        return ch;
      }
    }

    throw new IllegalStateException("No DcpChannel found for partition " + partition);
  }

  private void reconfigure(CouchbaseBucketConfig config) {
    final BucketConfigHelper configHelper = new BucketConfigHelper(config, env.sslEnabled());
    final boolean onlyConnectToPrimaryPartition = !env.persistencePollingEnabled();
    final List<NodeInfo> nodes = configHelper.getDataNodes(onlyConnectToPrimaryPartition);

    final Map<InetSocketAddress, DcpChannel> existingChannelsByAddress = channels.stream()
        .collect(toMap(DcpChannel::address, c -> c));

    final Set<InetSocketAddress> nodeAddresses = nodes.stream()
        .map(configHelper::getAddress)
        .collect(toSet());

    for (InetSocketAddress address : nodeAddresses) {
      if (!existingChannelsByAddress.containsKey(address)) {
        add(address);
      }
    }

    for (Map.Entry<InetSocketAddress, DcpChannel> entry : existingChannelsByAddress.entrySet()) {
      if (!nodeAddresses.contains(entry.getKey())) {
        remove(entry.getValue());
      }
    }
  }

  private void add(final InetSocketAddress node) {
    LOGGER.debug("Adding DCP Channel against {}", node);
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

    LOGGER.debug("Removing DCP Channel against {}", node);

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
  @SuppressWarnings("unchecked")
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
              ps.getLastUuid(),
              ps.getStartSeqno(),
              ps.getEndSeqno(),
              ps.getSnapshotStartSeqno(),
              ps.getSnapshotEndSeqno()
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
