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

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.conductor.BucketConfigSource;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.state.NotConnectedException;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;

public class PersistencePollingHandler extends ChannelInboundHandlerAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(PersistencePollingHandler.class);

  private static final LongAdder scheduledPollingTasks = requireNonNull(
      Metrics.globalRegistry.gauge("dcp.scheduled.polling.tasks", new LongAdder()));

  private final Client.Environment env;
  private final BucketConfigSource bucketConfigSource;
  private final DcpOps dcpOps;
  private final PersistedSeqnos persistedSeqnos;
  private final AtomicBoolean loggedClosureWarning = new AtomicBoolean();

  private Disposable configSubscription;

  // Incrementing this number causes any active polling tasks to stop.
  private int activeGroupId;

  public PersistencePollingHandler(final Client.Environment env,
                                   final BucketConfigSource bucketConfigSource,
                                   final DcpRequestDispatcher dispatcher) {
    this.env = requireNonNull(env);
    this.bucketConfigSource = requireNonNull(bucketConfigSource);
    this.persistedSeqnos = requireNonNull(env.persistedSeqnos());
    this.dcpOps = new DcpOpsImpl(dispatcher);
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    super.channelInactive(ctx);
    if (configSubscription != null) {
      // The null check seems weird, but it's necessary since channelActive
      // might not have been invoked if the connection wasn't fully established.
      configSubscription.dispose();
    }
    activeGroupId++; // cancel recurring polling tasks
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);

    configSubscription = bucketConfigSource.configs()
        .publishOn(Schedulers.fromExecutor(ctx.executor()))
        .subscribe(bucketConfig -> reconfigure(ctx, bucketConfig));
  }

  private void reconfigure(final ChannelHandlerContext ctx, final DcpBucketConfig bucketConfig) {
    LOGGER.debug("Reconfiguring persistence pollers.");

    // stops active group
    final int groupId = ++activeGroupId;

    this.persistedSeqnos.reset(bucketConfig);

    LOGGER.debug("Starting persistence polling group {}", groupId);

    try {
      for (PartitionInstance absentInstance : bucketConfig.getAbsentPartitionInstances()) {
        LOGGER.debug("Partition instance {} is absent, will assume all seqnos persisted.", absentInstance);
        this.persistedSeqnos.markAsAbsent(absentInstance);
      }

      final HostAndPort nodeAddress = DcpChannel.getHostAndPort(ctx.channel());
      final List<PartitionInstance> partitions = bucketConfig.getHostedPartitions(nodeAddress);

      LOGGER.debug("Node {} hosts partitions {}", nodeAddress, partitions);

      for (PartitionInstance partitionInstance : partitions) {
        final PartitionInstance pas = partitionInstance;
        dcpOps.getFailoverLog(partitionInstance.partition())
            .subscribe(failoverLog -> {
              long vbuuid = failoverLog.getCurrentVbuuid();
              observeAndRepeat(ctx, pas, vbuuid, groupId);
            }, throwable -> {
              if (throwable instanceof DcpOps.BadResponseStatusException) {
                // Common during rebalance. No need to spam the log with useless async stack trace.
                logWarningAndClose(ctx, "Failed to fetch failover log for {}. Server response: {}", pas, throwable.getMessage());
              } else {
                logWarningAndClose(ctx, "Failed to fetch failover log for {}.", pas, throwable);
              }
            });
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
      ctx.executor().schedule(() -> {
            scheduledPollingTasks.decrement();
            observeAndRepeat(ctx, partitionInstance, vbuuid, groupId);
          },
          env.persistencePollingIntervalMillis() * intervalMultiplier, TimeUnit.MILLISECONDS);

      scheduledPollingTasks.increment();

    } catch (Throwable t) {
      logWarningAndClose(ctx, "Failed to schedule observeSeqno.", t);
    }
  }

  private void observeAndRepeat(final ChannelHandlerContext ctx,
                                final PartitionInstance partitionInstance,
                                final long vbuuid,
                                final int groupId) {

    if (activeGroupId != groupId) {
      LOGGER.debug("Polling group {} is no longer active; stopping polling for {}", groupId, partitionInstance);
      return;
    }

    if (!env.streamEventBuffer().hasBufferedEvents(partitionInstance.partition())) {
      LOGGER.trace("No buffered events; skipping observeSeqno for partition instance {}", partitionInstance);
      scheduleObserveAndRepeat(ctx, partitionInstance, vbuuid, groupId, 1);
      return;
    }

    dcpOps.observeSeqno(partitionInstance.partition(), vbuuid)
        .doOnSuccess(observeSeqnoResponse -> {
          try {
            if (activeGroupId != groupId) {
              LOGGER.debug("Polling group {} is no longer active; stopping polling for {}", groupId, partitionInstance);
              return;
            }

            final long newVbuuid = observeSeqnoResponse.vbuuid();
            final long minSeqnoPersistedEverywhere = persistedSeqnos.update(partitionInstance, newVbuuid, observeSeqnoResponse.persistSeqno());
            env.streamEventBuffer().onSeqnoPersisted(observeSeqnoResponse.vbid(), minSeqnoPersistedEverywhere);
            scheduleObserveAndRepeat(ctx, partitionInstance, newVbuuid, groupId, 1);

          } catch (Throwable t) {
            logWarningAndClose(ctx, "Fatal error in observeAndRepeat handling observeSeqno response.", t);
          }
        })
        .doOnError(t -> {
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
        })
        .subscribe();
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
