/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.buffer;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.dcp.conductor.ConfigProvider;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.ChannelInboundHandlerAdapter;
import rx.SingleSubscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class PersistencePollingHandler extends ChannelInboundHandlerAdapter {

  private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(PersistencePollingHandler.class);

  private final ClientEnvironment env;
  private final ConfigProvider configProvider;
  private final DcpOps dcpOps;
  private final PersistedSeqnos persistedSeqnos;
  private final AtomicBoolean loggedClosureWarning = new AtomicBoolean();

  private Subscription configSubscription;

  // Incrementing this number causes any active polling tasks to stop.
  private int activeGroupId;

  public PersistencePollingHandler(final ClientEnvironment env,
                                   final ConfigProvider configProvider,
                                   final DcpRequestDispatcher dispatcher) {
    this.env = requireNonNull(env);
    this.configProvider = requireNonNull(configProvider);
    this.persistedSeqnos = requireNonNull(env.persistedSeqnos());
    this.dcpOps = new DcpOpsImpl(dispatcher);
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    if (configSubscription != null) {
      // The null check seems weird, but it's necessary since channelActive
      // might not have been invoked if the connection wasn't fully established.
      configSubscription.unsubscribe();
    }
    activeGroupId++; // cancel recurring polling tasks
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);

    configSubscription = configProvider.configs()
        .observeOn(Schedulers.from(ctx.executor()))
        .subscribe(bucketConfig -> reconfigure(ctx, bucketConfig));
  }

  private void reconfigure(final ChannelHandlerContext ctx, final CouchbaseBucketConfig bucketConfig) {
    LOGGER.debug("Reconfiguring persistence pollers.");

    // stops active group
    final int groupId = ++activeGroupId;

    this.persistedSeqnos.reset(bucketConfig);

    LOGGER.debug("Starting persistence polling group {}", groupId);

    try {
      final BucketConfigHelper bucketConfigHelper = new BucketConfigHelper(bucketConfig, env.sslEnabled());
      for (PartitionInstance absentInstance : bucketConfigHelper.getAbsentPartitionInstances()) {
        LOGGER.debug("Partition instance {} is absent, will assume all seqnos persisted.", absentInstance);
        this.persistedSeqnos.markAsAbsent(absentInstance);
      }

      final InetSocketAddress nodeAddress = (InetSocketAddress) ctx.channel().remoteAddress();
      final List<PartitionInstance> partitions = bucketConfigHelper.getHostedPartitions(nodeAddress);

      LOGGER.debug("Node {} hosts partitions {}", nodeAddress, partitions);

      for (PartitionInstance partitionInstance : partitions) {
        final PartitionInstance pas = partitionInstance;
        dcpOps.getFailoverLog(partitionInstance.partition())
            .subscribe(failoverLog -> {
              long vbuuid = failoverLog.getCurrentVbuuid();
              observeAndRepeat(ctx, pas, vbuuid, groupId);
            }, throwable -> logWarningAndClose(ctx, "Failed to fetch failover log for {}.", pas, throwable));
      }
    } catch (Throwable t) {
      logWarningAndClose(ctx, "Failed to reconfigure persistence poller.", t);
    }
  }

  private void scheduleObserveAndRepeat(final ChannelHandlerContext ctx,
                                        final PartitionInstance partitionInstance,
                                        final long vbuuid,
                                        final int groupId,
                                        final int intervalMultiplier) {
    if (intervalMultiplier < 1) {
      throw new IllegalArgumentException("Interval multiplier must be > 0");
    }

    try {
      ctx.executor().schedule(() -> observeAndRepeat(ctx, partitionInstance, vbuuid, groupId),
          env.persistencePollingIntervalMillis() * intervalMultiplier, TimeUnit.MILLISECONDS);

    } catch (Throwable t) {
      logWarningAndClose(ctx, "Failed to schedule observeSeqno.", t);
    }
  }

  private void observeAndRepeat(final ChannelHandlerContext ctx,
                                final PartitionInstance partitionInstance,
                                final long vbuuid,
                                final int groupId) {

    if (!env.streamEventBuffer().hasBufferedEvents(partitionInstance.partition())) {
      LOGGER.trace("No buffered events; skipping observeSeqno for partition instance {}", partitionInstance);
      scheduleObserveAndRepeat(ctx, partitionInstance, vbuuid, groupId, 1);
      return;
    }

    dcpOps.observeSeqno(partitionInstance.partition(), vbuuid).subscribe(new SingleSubscriber<ObserveSeqnoResponse>() {
      @Override
      public void onSuccess(ObserveSeqnoResponse observeSeqnoResponse) {
        try {
          if (activeGroupId != groupId) {
            LOGGER.debug("Polling group {} is no longer active; stopping polling for ", groupId, partitionInstance);
            return;
          }

          final long newVbuuid = observeSeqnoResponse.vbuuid();
          final long minSeqnoPersistedEverywhere = persistedSeqnos.update(partitionInstance, newVbuuid, observeSeqnoResponse.persistSeqno());
          env.streamEventBuffer().onSeqnoPersisted(observeSeqnoResponse.vbid(), minSeqnoPersistedEverywhere);
          scheduleObserveAndRepeat(ctx, partitionInstance, newVbuuid, groupId, 1);

        } catch (Throwable t) {
          logWarningAndClose(ctx, "Fatal error in observeAndRepeat handling observeSeqno response.", t);
        }
      }

      @Override
      public void onError(Throwable t) {
        if (activeGroupId != groupId || t instanceof NotConnectedException) {
          // Graceful shutdown. Ignore any exception.
          LOGGER.debug("Polling group {} is no longer active; stopping polling for {}",
              groupId, partitionInstance);
          return;
        }

        if (t instanceof DcpOps.BadResponseStatusException) {
          DcpOps.BadResponseStatusException e = (DcpOps.BadResponseStatusException) t;
          if (e.status().isTemporary()) {
            LOGGER.debug("observeSeqno failed with status code " + e.status() + " ; will retry after an extended delay.");
            // schedule with an extended delay
            scheduleObserveAndRepeat(ctx, partitionInstance, vbuuid, groupId, 10);
          } else {
            logWarningAndClose(ctx, "observeSeqno failed with status code " + e.status());
          }
        } else {
          logWarningAndClose(ctx, "observeSeqno failed.", t);
        }
      }
    });
  }

  private void logWarningAndClose(ChannelHandlerContext ctx, String msg, Object... params) {
    if (loggedClosureWarning.compareAndSet(false, true)) {
      LOGGER.warn("Closing channel; " + msg, params);
      ctx.close();
    } else {
      LOGGER.trace("Closing channel; " + msg, params);
    }
  }
}
