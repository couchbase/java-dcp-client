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
package com.couchbase.client.dcp;

import com.couchbase.client.dcp.buffer.PersistedSeqnos;
import com.couchbase.client.dcp.buffer.StreamEventBuffer;
import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.env.NetworkResolution;
import com.couchbase.client.dcp.core.event.EventBus;
import com.couchbase.client.dcp.core.event.EventType;
import com.couchbase.client.dcp.core.time.Delay;
import com.couchbase.client.dcp.core.utils.ConnectionString;
import com.couchbase.client.dcp.error.BootstrapException;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.events.DefaultDcpEventBus;
import com.couchbase.client.dcp.events.LoggingTracer;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.events.Tracer;
import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;
import com.couchbase.client.dcp.highlevel.DocumentChange;
import com.couchbase.client.dcp.highlevel.FlowControlMode;
import com.couchbase.client.dcp.highlevel.SnapshotMarker;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.client.dcp.highlevel.internal.AsyncEventDispatcher;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.EventHandlerAdapter;
import com.couchbase.client.dcp.highlevel.internal.ImmediateEventDispatcher;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSeqnoAdvancedRequest;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpSystemEvent;
import com.couchbase.client.dcp.message.DcpSystemEventRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.OpenConnectionFlag;
import com.couchbase.client.dcp.message.PartitionAndSeqno;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.metrics.DcpClientMetrics;
import com.couchbase.client.dcp.metrics.LogLevel;
import com.couchbase.client.dcp.metrics.MetricsContext;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.meta;
import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.core.utils.CbCollections.isNullOrEmpty;
import static com.couchbase.client.dcp.highlevel.FlowControlMode.AUTOMATIC;
import static com.couchbase.client.dcp.metrics.LogLevel.NONE;
import static com.couchbase.client.dcp.util.MathUtils.lessThanUnsigned;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;

/**
 * This {@link Client} provides the main API to configure and use the DCP client.
 * <p>
 * Create a new instance using the builder returned by the {@link #builder()} method.
 */
public class Client implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

  /**
   * Thread factory for default event loop group. Static so the thread name counter
   * is shared if multiple clients are created (although in that case the user
   * should ideally provide their own event loop group).
   */
  private static final ThreadFactory threadFactory = new DefaultThreadFactory("dcp-io");

  /**
   * The {@link Conductor} handles channels and streams. It's the orchestrator of everything.
   */
  private final Conductor conductor;

  /**
   * The stateful {@link Environment}, used internally for centralized config management.
   */
  private final Environment env;

  /**
   * If buffer acknowledgment is enabled.
   */
  private final boolean bufferAckEnabled;

  /**
   * (nullable) Dispatches events for listeners registered via
   * {@link #listener(DatabaseChangeListener, FlowControlMode)}.
   */
  private volatile AsyncEventDispatcher listenerDispatcher;

  /**
   * Prevents user from attempting to register multiple listeners.
   */
  private final AtomicBoolean hasHighLevelListener = new AtomicBoolean();

  /**
   * Creates a new {@link Client} instance.
   *
   * @param builder the client config builder.
   */
  private Client(Builder builder) {
    env = new Environment(builder);

    bufferAckEnabled = env.dcpControl().bufferAckEnabled();
    if (bufferAckEnabled) {
      if (env.bufferAckWatermark() == 0) {
        throw new IllegalArgumentException("The bufferAckWatermark needs to be set if bufferAck is enabled.");
      }
    }

    // Provide minimal default event handlers
    controlEventHandler((flowController, event) -> {
      try {
        if (DcpSnapshotMarkerRequest.is(event)) {
          flowController.ack(event);
        }
      } finally {
        event.release();
      }
    });
    dataEventHandler((flowController, event) -> {
      try {
        flowController.ack(event);
      } finally {
        event.release();
      }
    });

    MetricsContext metricsContext = new MetricsContext(builder.meterRegistry, "dcp");
    conductor = new Conductor(env, new DcpClientMetrics(metricsContext));
    LOGGER.info("Environment Configuration Used: {}", system(env));
  }

  private static EventLoopGroup newEventLoopGroup() {
    if (Epoll.isAvailable()) {
      LOGGER.info("Using Netty epoll native transport.");
      return new EpollEventLoopGroup(threadFactory);
    }

    if (KQueue.isAvailable()) {
      LOGGER.info("Using Netty kqueue native transport.");
      return new KQueueEventLoopGroup(threadFactory);
    }

    LOGGER.info("Using Netty NIO transport.");
    return new NioEventLoopGroup(threadFactory);
  }

  /**
   * Allows to configure the {@link Client} before bootstrap through a {@link Builder}.
   *
   * @return the builder to configure the client.
   * @deprecated in favor of {@link #builder}
   */
  @Deprecated
  public static Builder configure() {
    return builder();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Get the current sequence numbers from all partitions.
   *
   * @return an {@link Flux} of partition and sequence number.
   */
  public Flux<PartitionAndSeqno> getSeqnos() {
    return conductor.getSeqnos();
  }

  /**
   * Returns the current {@link SessionState}, useful for persistence and inspection.
   *
   * @return the current session state.
   */
  public SessionState sessionState() {
    return conductor.sessionState();
  }

  /**
   * Registers an event listener that will be invoked in a dedicated thread.
   * The listener's methods may safely block without disrupting the Netty I/O layer.
   */
  public void listener(DatabaseChangeListener listener, FlowControlMode flowControlMode) {
    if (!hasHighLevelListener.compareAndSet(false, true)) {
      throw new IllegalStateException("Listener may only be set once.");
    }

    if (flowControlMode == AUTOMATIC && !bufferAckEnabled) {
      throw new IllegalStateException("Can't register listener in automatic flow control mode because the DCP client was not configured for flow control." +
          " Make sure to call flowControl(bufferSizeInBytes) when building the DCP client.");
    }

    this.listenerDispatcher = new AsyncEventDispatcher(flowControlMode, listener);
    EventHandlerAdapter.register(this, listenerDispatcher);
  }

  /**
   * Registers an event listener to be invoked in the Netty I/O event loop thread.
   * The listener's methods should return quickly and <b>MUST NOT</b> block.
   * <p>
   * Non-blocking listeners always use {@link FlowControlMode#MANUAL}.
   */
  public void nonBlockingListener(DatabaseChangeListener listener) {
    if (!hasHighLevelListener.compareAndSet(false, true)) {
      throw new IllegalStateException("Listener may only be set once.");
    }

    EventHandlerAdapter.register(this, new ImmediateEventDispatcher(listener));
  }

  /**
   * Stores a {@link ControlEventHandler} to be called when control events happen.
   * <p>
   * All events (passed as {@link ByteBuf}s) that the callback receives need to be handled
   * and at least released (by using {@link ByteBuf#release()}, otherwise they will leak.
   * <p>
   * The following messages can happen and should be handled depending on the needs of the
   * client:
   * <p>
   * - {@link RollbackMessage}: If during a connect phase the server responds with rollback
   * information, this event is forwarded to the callback. Does not need to be acknowledged.
   * <p>
   * - {@link DcpSnapshotMarkerRequest}: Server transmits data in batches called snapshots
   * before sending anything, it send marker message, which contains start and end sequence
   * numbers of the data in it. Need to be acknowledged.
   * <p>
   * Keep in mind that the callback is executed on the IO thread (netty's thread pool for the
   * event loops) so further synchronization is needed if the data needs to be used on a different
   * thread in a thread safe manner.
   *
   * @param controlEventHandler the event handler to use.
   */
  public void controlEventHandler(final ControlEventHandler controlEventHandler) {
    final boolean userHandlerWantsFailoverLogs = controlEventHandler instanceof EventHandlerAdapter;

    env.setControlEventHandler(new ControlEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpSnapshotMarkerRequest.is(event)) {
          // Keep snapshot information in the session state, but also forward event to user
          int partition = DcpSnapshotMarkerRequest.partition(event);
          sessionState().get(partition)
              .setSnapshot(new SnapshotMarker(
                  DcpSnapshotMarkerRequest.startSeqno(event),
                  DcpSnapshotMarkerRequest.endSeqno(event)));
        } else if (DcpFailoverLogResponse.is(event)) {
          handleFailoverLogResponse(event);

          if (!userHandlerWantsFailoverLogs) {
            // Do not forward failover log responses for now since their info is kept
            // in the session state transparently
            event.release();
            return;
          }
        } else if (RollbackMessage.is(event)) {
          // even if forwarded to the user, warn in case the user is not
          // aware of rollback messages.
          LOGGER.warn(
              "Received rollback for vbucket {} to seqno {}",
              RollbackMessage.vbucket(event),
              RollbackMessage.seqno(event)
          );
        } else if (DcpSeqnoAdvancedRequest.is(event)) {
          handleSeqnoAdvanced(event);

        } else if (DcpSystemEventRequest.is(event)) {
          handleDcpSystemEvent(event);
        }

        // Forward event to user.
        controlEventHandler.onEvent(flowController, event);
      }

      private void handleSeqnoAdvanced(ByteBuf event) {
        final int vbucket = MessageUtil.getVbucket(event);
        final long seqno = DcpSeqnoAdvancedRequest.getSeqno(event);
        LOGGER.debug("Seqno for vbucket {} advanced to {}", vbucket, seqno);

        PartitionState ps = sessionState().get(vbucket);
        ps.setStartSeqno(seqno);
        ps.setSnapshot(new SnapshotMarker(seqno, seqno));
      }

      private void handleDcpSystemEvent(ByteBuf event) {
        final long seqno = DcpSystemEventRequest.getSeqno(event);
        final int vbucket = MessageUtil.getVbucket(event);

        final PartitionState ps = sessionState().get(vbucket);
        ps.setStartSeqno(seqno);

        final DcpSystemEvent sysEvent = DcpSystemEvent.parse(event);
        if (!(sysEvent instanceof DcpSystemEvent.CollectionsManifestEvent)) {
          LOGGER.warn("Ignoring unrecognized DCP system event!\n{}", meta(MessageUtil.humanize(event)));

        } else {
          final DcpSystemEvent.CollectionsManifestEvent manifestEvent = (DcpSystemEvent.CollectionsManifestEvent) sysEvent;
          final CollectionsManifest existingManifest = ps.getCollectionsManifest();

          // remember the UID so it can be included in subsequent stream offsets
          ps.setCollectionsManifestUid(manifestEvent.getManifestId());

          // If the manifest IDs are equal, the update might still be relevant since
          // multiple events may have the same manifest ID. So only ignore if the event's UID
          // is less than the
          if (lessThanUnsigned(manifestEvent.getManifestId(), existingManifest.getId())) {
            LOGGER.debug("Ignoring collection manifest event; UID {} is < current manifest UID {}",
                manifestEvent.getManifestId(), existingManifest.getId());
          } else {
            // If the server has caught up with the manifest we fetched when opening the stream,
            // this *might* be a redundant change. That's okay because the collections manifest
            // class allows redundant modifications.
            LOGGER.debug("Applying collection manifest change; UID {} is >= current manifest UID {}",
                manifestEvent.getManifestId(), existingManifest.getId());

            ps.setCollectionsManifest(manifestEvent.apply(existingManifest));
          }
        }
      }
    });
  }

  /**
   * Stores a {@link SystemEventHandler} to be called when control events happen.
   */
  public void systemEventHandler(final SystemEventHandler systemEventHandler) {
    env.setSystemEventHandler(systemEventHandler);
  }

  /**
   * Helper method to handle a failover log response.
   *
   * @param event the buffer representing the {@link DcpFailoverLogResponse}.
   */
  private void handleFailoverLogResponse(final ByteBuf event) {
    int partition = DcpFailoverLogResponse.vbucket(event);
    sessionState().get(partition)
        .setFailoverLog(DcpFailoverLogResponse.entries(event));
  }

  /**
   * Stores a {@link DataEventHandler} to be called when data events happen.
   * <p>
   * All events (passed as {@link ByteBuf}s) that the callback receives need to be handled
   * and at least released (by using {@link ByteBuf#release()}, otherwise they will leak.
   * <p>
   * The following messages can happen and should be handled depending on the needs of the
   * client:
   * <p>
   * - {@link DcpMutationMessage}: A mutation has occurred. Needs to be acknowledged.
   * - {@link DcpDeletionMessage}: A deletion has occurred. Needs to be acknowledged.
   * - {@link DcpExpirationMessage}: An expiration has occurred. Note that current server versions
   * (as of 4.5.0) are not emitting this event, but in any case you should at least release it to
   * be forwards compatible. Needs to be acknowledged.
   * <p>
   * Keep in mind that the callback is executed on the IO thread (netty's thread pool for the
   * event loops) so further synchronization is needed if the data needs to be used on a different
   * thread in a thread safe manner.
   *
   * @param dataEventHandler the event handler to use.
   */
  public void dataEventHandler(final DataEventHandler dataEventHandler) {
    env.setDataEventHandler((flowController, event) -> {
      if (DcpMutationMessage.is(event) || DcpDeletionMessage.is(event) || DcpExpirationMessage.is(event)) {
        int partition = MessageUtil.getVbucket(event);
        long seqno = DcpMutationMessage.bySeqno(event); // works for deletion and expiry too
        sessionState().get(partition)
            .setStartSeqno(seqno);
      }

      // Forward event to user.
      dataEventHandler.onEvent(flowController, event);
    });
  }

  /**
   * Initializes the underlying connections (not the streams) and sets up everything as needed.
   *
   * @return a {@link Mono} signaling that the connect phase has been completed or failed.
   */
  public Mono<Void> connect() {
    if (!conductor.disconnected()) {
      // short-circuit connect attempt if the conductor is already connecting/connected.
      LOGGER.debug("Ignoring duplicate connect attempt, already connecting/connected.");
      return Mono.empty();
    }

    if (env.dataEventHandler() == null) {
      throw new IllegalArgumentException("A DataEventHandler needs to be provided!");
    }
    if (env.controlEventHandler() == null) {
      throw new IllegalArgumentException("A ControlEventHandler needs to be provided!");
    }
    LOGGER.info("Connecting to seed nodes and bootstrapping bucket {}.", meta(env.bucket()));
    return conductor.connect().onErrorResume(throwable ->
        disconnect()
            .then(Mono.error(new BootstrapException("Could not connect to Cluster/Bucket", throwable))));
  }

  /**
   * Disconnect the {@link Client} and shut down all its owned resources.
   * <p>
   * If custom state is used (like a shared {@link EventLoopGroup}), then they must be closed and managed
   * separately after this disconnect process has finished.
   *
   * @return a {@link Mono} signaling that the disconnect phase has been completed or failed.
   */
  public Mono<Void> disconnect() {
    return dispatcherGracefulShutdown()
        .then(conductor.stop())
        .then(env.shutdown())
        .then(dispatcherAwaitShutdown());
  }

  /**
   * Disconnects the client. This blocking alternative to {@link #disconnect()}
   * exists to support try-with-resources.
   */
  @Override
  public void close() {
    disconnect().block(Duration.ofSeconds(60));
  }

  private Mono<Void> dispatcherGracefulShutdown() {
    return Mono.fromRunnable(() -> {
      if (listenerDispatcher != null) {
        LOGGER.info("Asking event dispatcher to shut down.");
        listenerDispatcher.gracefulShutdown();
      }
    });
  }

  private Mono<Void> dispatcherAwaitShutdown() {
    return Mono.fromCallable(() -> {
      final long startNanos = System.nanoTime();
      if (listenerDispatcher != null) {
        if (!listenerDispatcher.awaitTermination(Duration.ofSeconds(30))) {
          LOGGER.info("Forcing event dispatcher to shut down.");
          listenerDispatcher.shutdownNow();
          if (!listenerDispatcher.awaitTermination(Duration.ofSeconds(10))) {
            LOGGER.warn("Event dispatcher still hasn't terminated after {} seconds.",
                NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
          }
        }
      }
      return null;
    })
        .then()
        .subscribeOn(Schedulers.elastic());
  }

  /**
   * @param vbucketToOffset a Map where each key is a vbucket to stream, and the value is the offset from which to resume streaming.
   * @return a {@link Mono} that completes when the streams have been successfully opened.
   */
  public Mono<Void> resumeStreaming(Map<Integer, StreamOffset> vbucketToOffset) {
    if (vbucketToOffset.isEmpty()) {
      return Mono.empty();
    }

    vbucketToOffset.forEach((partition, offset) ->
        sessionState().set(partition, PartitionState.fromOffset(offset)));

    return startStreaming(vbucketToOffset.keySet());
  }

  private static List<Integer> toIntList(Short... shorts) {
    List<Integer> result = new ArrayList<>();
    for (Short s : shorts) {
      result.add((int) s);
    }
    return result;
  }

  /**
   * @deprecated Please use {@link #startStreaming(Collection)} instead.
   */
  @Deprecated
  public Mono<Void> startStreaming(Short... vbids) {
    return startStreaming(toIntList(vbids));
  }

  /**
   * Start streaming for all partitions.
   *
   * @return a {@link Mono} indicating that streaming has started or failed.
   */
  public Mono<Void> startStreaming() {
    return startStreaming(emptyList());
  }

  /**
   * Start DCP streams based on the initialized state for the given partition IDs (vbids).
   * <p>
   * If no ids are provided, all initialized partitions will be started.
   *
   * @param vbids the partition ids (0-indexed) to start streaming for.
   * @return a {@link Mono} indicating that streaming has started or failed.
   */
  public Mono<Void> startStreaming(Collection<Integer> vbids) {
    int numPartitions = numPartitions();
    final List<Integer> partitions = partitionsForVbids(numPartitions, vbids);

    List<Integer> initializedPartitions = selectInitializedPartitions(numPartitions, partitions);

    List<Integer> noopPartitions = new ArrayList<>();
    for (int p : partitions) {
      if (!initializedPartitions.contains(p)) {
        noopPartitions.add(p);
      }
    }
    if (!noopPartitions.isEmpty()) {
      LOGGER.info("Immediately sending stream end events for {} partitions already at desired end.", noopPartitions.size());
      LOGGER.debug("Immediately sending stream end events for partitions already at desired end: {}", noopPartitions);
      noopPartitions.forEach(p ->
          env.eventBus().publish(
              new StreamEndEvent(p, StreamEndReason.OK)));
    }

    if (initializedPartitions.isEmpty()) {
      LOGGER.info("The configured session state does not require any streams to be opened. Completing immediately.");
      return Mono.empty();
    }

    LOGGER.info("Starting to Stream for " + initializedPartitions.size() + " partitions");
    LOGGER.debug("Stream start against partitions: {}", initializedPartitions);

    return Flux
        .fromIterable(initializedPartitions)
        .flatMap(partition -> {
          PartitionState partitionState = sessionState().get(partition);
          return conductor.startStreamForPartition(
              partition,
              partitionState.getOffset(),
              partitionState.getEndSeqno()
          ).onErrorResume(throwable ->
              (throwable instanceof RollbackException)
                  ? Mono.empty() // Ignore rollbacks since they are handled out of band.
                  : Mono.error(throwable));
        })
        .then();
  }

  /**
   * Helper method to check on stream start that some kind of state is initialized to avoid a common error
   * of starting without initializing.
   */
  private List<Integer> selectInitializedPartitions(int clusterPartitions, List<Integer> partitions) {
    List<Integer> initializedPartitions = new ArrayList<>();
    SessionState state = sessionState();

    for (int partition : partitions) {
      PartitionState ps = state.get(partition);
      if (ps != null) {
        if (lessThanUnsigned(ps.getStartSeqno(), ps.getEndSeqno())) {
          initializedPartitions.add(partition);
        } else {
          LOGGER.debug("Skipping partition {}, because startSeqno({}) >= endSeqno({})",
              partition, ps.getStartSeqno(), ps.getEndSeqno());
        }
      } else {
        LOGGER.debug("Skipping partition {}, because its state is null", partition);
      }
    }

    if (initializedPartitions.size() > clusterPartitions) {
      throw new IllegalStateException("Session State has " + initializedPartitions
          + " partitions while the cluster has " + clusterPartitions + "!");
    }
    return initializedPartitions;
  }

  /**
   * @deprecated Please use {@link #stopStreaming(Collection)} instead.
   */
  @Deprecated
  public Mono<Void> stopStreaming(Short... vbids) {
    return stopStreaming(toIntList(vbids));
  }

  /**
   * Stop DCP streams for the given partition IDs (vbids).
   * <p>
   * If no ids are provided, all partitions will be stopped. Note that you can also use this to "pause" streams
   * if {@link #startStreaming} is called later - since the session state is persisted and streaming
   * will resume from the current position.
   *
   * @param vbids the partition ids (0-indexed) to stop streaming for.
   * @return a {@link Mono} indicating that streaming has stopped or failed.
   */
  public Mono<Void> stopStreaming(Collection<Integer> vbids) {
    List<Integer> partitions = partitionsForVbids(numPartitions(), vbids);

    return Flux.fromIterable(partitions)
        .doOnSubscribe(subscription -> {
          LOGGER.info("Stopping stream for {} partitions", partitions.size());
          LOGGER.debug("Stream stop against partitions: {}", partitions);
        })
        .flatMap(conductor::stopStreamForPartition)
        .then();
  }

  /**
   * Helper method to turn the array of vbids into a list.
   *
   * @param numPartitions the number of partitions on the cluster as a fallback.
   * @param vbids the potentially empty array of selected vbids.
   * @return a sorted list of partitions to use.
   */
  private static List<Integer> partitionsForVbids(int numPartitions, Collection<Integer> vbids) {
    if (!vbids.isEmpty()) {
      List<Integer> result = new ArrayList<>(vbids);
      Collections.sort(result);
      return result;
    }

    List<Integer> partitions = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(i);
    }
    return partitions;
  }

  /**
   * @deprecated Please use {@link #failoverLogs(Collection)} instead.
   */
  @Deprecated
  public Flux<ByteBuf> failoverLogs(Short... vbids) {
    return failoverLogs(toIntList(vbids));
  }

  /**
   * Helper method to return the failover logs for the given partitions (vbids).
   * <p>
   * If the list is empty, the failover logs for all partitions will be returned. Note that the returned
   * ByteBufs can be analyzed using the {@link DcpFailoverLogResponse} flyweight.
   *
   * @param vbids the partitions to return the failover logs from.
   * @return an {@link Flux} containing all failover logs.
   */
  public Flux<ByteBuf> failoverLogs(Collection<Integer> vbids) {
    List<Integer> partitions = partitionsForVbids(numPartitions(), vbids);

    LOGGER.debug("Asking for failover logs on partitions {}", partitions);

    return Flux.fromIterable(partitions)
        .flatMap(conductor::getFailoverLog);
  }

  public Mono<ByteBuf> failoverLog(int vbid) {
    return Mono.just(vbid).flatMap(conductor::getFailoverLog);
  }

  /**
   * Helper method to rollback the partition state and stop/restart the stream.
   * <p>
   * The stream is stopped (if not already done). Then:
   * <p>
   * The rollback seqno state is applied. Note that this will also remove all the failover logs for the partition
   * that are higher than the given seqno, since the server told us we are ahead of it.
   * <p>
   * Finally, the stream is restarted again.
   *
   * @param partition the partition id
   * @param seqno the sequence number to rollback to
   */
  public Mono<Void> rollbackAndRestartStream(final int partition, final long seqno) {
    return stopStreaming(singletonList(partition))
        .then(Mono.fromRunnable(() -> sessionState().rollbackToPosition(partition, seqno)))
        .then(startStreaming(singletonList(partition)));
  }


  /**
   * Returns the number of partitions on the remote cluster.
   * <p>
   * Note that you must be connected, since the information is loaded from the server configuration.
   * On all OS'es other than OSX it will be 1024, on OSX it is 64. Treat this as an opaque value anyways.
   *
   * @return the number of partitions (vbuckets).
   */
  public int numPartitions() {
    return conductor.numberOfPartitions();
  }


  /**
   * Returns true if the stream for the given partition id is currently open.
   *
   * @param vbid the partition id.
   * @return true if it is open, false otherwise.
   */
  public boolean streamIsOpen(int vbid) {
    return conductor.streamIsOpen(vbid);
  }

  /**
   * Initialize the {@link SessionState} based on arbitrary time points.
   * <p>
   * The following combinations are supported and make sense:
   * <p>
   * - {@link StreamFrom#BEGINNING} to {@link StreamTo#NOW}
   * - {@link StreamFrom#BEGINNING} to {@link StreamTo#INFINITY}
   * - {@link StreamFrom#NOW} to {@link StreamTo#INFINITY}
   * <p>
   * If you already have state captured and you want to resume from this position, use
   * {@link #recoverState(StateFormat, byte[])} or {@link #recoverOrInitializeState(StateFormat, byte[], StreamFrom, StreamTo)}
   * instead.
   *
   * @param from where to start streaming from.
   * @param to when to stop streaming.
   * @return A {@link Mono} indicating the success or failure of the state init.
   */
  public Mono<Void> initializeState(final StreamFrom from, final StreamTo to) {
    if (from == StreamFrom.BEGINNING && to == StreamTo.INFINITY) {
      buzzMe();
      return initFromBeginningToInfinity();
    } else if (from == StreamFrom.BEGINNING && to == StreamTo.NOW) {
      return initFromBeginningToNow();
    } else if (from == StreamFrom.NOW && to == StreamTo.INFINITY) {
      buzzMe();
      return initFromNowToInfinity();
    } else {
      throw new IllegalStateException("Unsupported FROM/TO combination: " + from + " -> " + to);
    }
  }

  /**
   * Initializes the {@link SessionState} from a previous snapshot with specific state information.
   * <p>
   * If a system needs to be built that withstands outages and needs to resume where left off, this method,
   * combined with the periodic persistence of the {@link SessionState} provides resume capabilities. If you
   * need to start fresh, take a look at {@link #initializeState(StreamFrom, StreamTo)} as well as
   * {@link #recoverOrInitializeState(StateFormat, byte[], StreamFrom, StreamTo)}.
   *
   * @param format the format used when persisting.
   * @param persistedState the opaque byte array representing the persisted state.
   * @return A {@link Mono} indicating the success or failure of the state recovery.
   */
  public Mono<Void> recoverState(final StateFormat format, final byte[] persistedState) {
    return Mono.create(sink -> {
      LOGGER.info("Recovering state from format {}", format);
      LOGGER.debug("PersistedState on recovery is: {}", new String(persistedState, StandardCharsets.UTF_8));

      try {
        if (format == StateFormat.JSON) {
          sessionState().setFromJson(persistedState);
          sink.success();
        } else {
          sink.error(new IllegalStateException("Unsupported StateFormat " + format));
        }
      } catch (Exception ex) {
        sink.error(ex);
      }
    });
  }

  /**
   * Recovers or initializes the {@link SessionState}.
   * <p>
   * This method is a convience wrapper around initialization and recovery. It combines both methods and
   * checks if the persisted state byte array is null or empty and if so it starts with the params given. If
   * it is not empty it recovers from there. This acknowledges the fact that ideally the state is persisted
   * somewhere but if its not there you want to start at a specific point in time.
   *
   * @param format the persistence format used.
   * @param persistedState the state, may be null or empty.
   * @param from from where to start streaming if persisted state is null or empty.
   * @param to to where to stream if persisted state is null or empty.
   * @return A {@link Mono} indicating the success or failure of the state recovery or init.
   */
  public Mono<Void> recoverOrInitializeState(final StateFormat format, final byte[] persistedState,
                                             final StreamFrom from, final StreamTo to) {
    if (persistedState == null || persistedState.length == 0) {
      return initializeState(from, to);
    } else {
      return recoverState(format, persistedState);
    }
  }


  /**
   * Initializes the session state from beginning to no end.
   */
  private Mono<Void> initFromBeginningToInfinity() {
    return Mono.create(sink -> {
      LOGGER.info("Initializing state from beginning to no end.");

      try {
        sessionState().setToBeginningWithNoEnd(numPartitions());
        sink.success();
      } catch (Exception ex) {
        LOGGER.warn("Failed to initialize state from beginning to no end.", ex);
        sink.error(ex);
      }
    });
  }

  /**
   * Initializes the session state from now to no end.
   */
  private Mono<Void> initFromNowToInfinity() {
    return initWithCallback(partitionAndSeqno -> {
      int partition = partitionAndSeqno.partition();
      long seqno = partitionAndSeqno.seqno();
      PartitionState partitionState = sessionState().get(partition);
      partitionState.setStartSeqno(seqno);
      partitionState.setSnapshot(new SnapshotMarker(seqno, seqno));
      sessionState().set(partition, partitionState);
    });
  }

  /**
   * Initializes the session state from beginning to now.
   */
  private Mono<Void> initFromBeginningToNow() {
    return initWithCallback(partitionAndSeqno -> {
      int partition = partitionAndSeqno.partition();
      long seqno = partitionAndSeqno.seqno();
      PartitionState partitionState = sessionState().get(partition);
      partitionState.setEndSeqno(seqno);
      sessionState().set(partition, partitionState);
    });
  }

  /**
   * Helper method to initialize all kinds of states.
   * <p>
   * This method grabs the sequence numbers and then calls a callback for customization. Once that is done it
   * grabs the failover logs and populates the session state with the failover log information.
   */
  private Mono<Void> initWithCallback(Consumer<PartitionAndSeqno> callback) {
    sessionState().setToBeginningWithNoEnd(numPartitions());

    return getSeqnos()
        .doOnNext(callback)
        .map(PartitionAndSeqno::partition)
        .flatMap(this::failoverLog)
        .map(buf -> {
          int partition = DcpFailoverLogResponse.vbucket(buf);
          handleFailoverLogResponse(buf);
          buf.release();
          return partition;
        })
        .then();
  }

  /**
   * Builder object to customize the {@link Client} creation.
   */
  public static class Builder {
    private List<HostAndPort> seedNodes = singletonList(new HostAndPort("127.0.0.1", 0));
    private NetworkResolution networkResolution = NetworkResolution.AUTO;
    private EventLoopGroup eventLoopGroup;
    private String bucket = "default";
    private boolean collectionsAware;
    private Set<Long> collectionIds = new HashSet<>();
    private Set<String> collectionNames = new HashSet<>();
    private OptionalLong scopeId = OptionalLong.empty();
    private Optional<String> scopeName = Optional.empty();
    private Authenticator authenticator = null;
    private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
    private final DcpControl dcpControl = new DcpControl()
        .put(DcpControl.Names.ENABLE_NOOP, "true"); // required for collections, and a good idea anyway
    private final EnumSet<OpenConnectionFlag> connectionFlags = EnumSet.noneOf(OpenConnectionFlag.class);
    private int bufferAckWatermark;
    private boolean poolBuffers = true;
    private Duration bootstrapTimeout = Environment.DEFAULT_BOOTSTRAP_TIMEOUT;
    private Duration configRefreshInterval = Environment.DEFAULT_CONFIG_REFRESH_INTERVAL;
    private long socketConnectTimeout = Environment.DEFAULT_SOCKET_CONNECT_TIMEOUT;
    private int dcpChannelsReconnectMaxAttempts = Environment.DEFAULT_DCP_CHANNELS_RECONNECT_MAX_ATTEMPTS;
    private Retry dcpChannelsReconnectDelay = Environment.DEFAULT_DCP_CHANNELS_RECONNECT_DELAY;
    private EventBus eventBus;
    private SecurityConfig securityConfig = SecurityConfig.builder().build();
    private long persistencePollingIntervalMillis;
    private MeterRegistry meterRegistry = Metrics.globalRegistry;
    private Tracer tracer = Tracer.NOOP;

    /**
     * If the argument is true, configures the client to receive only
     * document keys and metadata (no contents).
     * <p>
     * Defaults to false.
     *
     * @return this {@link Builder} for nice chainability.
     */
    public Builder noValue(boolean noValue) {
      return setConnectionFlag(OpenConnectionFlag.NO_VALUE, noValue);
    }

    /**
     * If the argument is true, configures the client to receive
     * extended attributes (XATTRS).
     * <p>
     * Defaults to false.
     * <p>
     * If set to true, users of the low-level API may call
     * {@link MessageUtil#getContentAndXattrs(ByteBuf)}
     * to parse the content and XATTRs of mutation requests.
     * Users of the high-level API may view XATTRs by calling
     * {@link DocumentChange#getXattrs()}.
     *
     * @return this {@link Builder} for nice chainability.
     */
    public Builder xattrs(boolean xattrs) {
      return setConnectionFlag(OpenConnectionFlag.INCLUDE_XATTRS, xattrs);
    }

    private Builder setConnectionFlag(OpenConnectionFlag flag, boolean value) {
      if (value) {
        connectionFlags.add(flag);
      } else {
        connectionFlags.remove(flag);
      }
      return this;
    }

    /**
     * The buffer acknowledge watermark in percent.
     *
     * @param watermark between 0 and 100, needs to be > 0 if flow control is enabled.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder bufferAckWatermark(int watermark) {
      if (watermark > 100 || watermark < 0) {
        throw new IllegalArgumentException("The bufferAckWatermark is percents, so it needs to be between" +
            " 0 and 100");
      }
      this.bufferAckWatermark = watermark;
      return this;
    }

    /**
     * Sets the addresses of the Couchbase Server nodes to bootstrap against.
     * <p>
     * If a port is specified, it must be the KV service port.
     * <p>
     * The port may be omitted if Couchbase is listening on the standard KV ports
     * (11210 and 11207 for insecure and TLS connections, respectively).
     *
     * @param addresses seed nodes.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder seedNodes(final Collection<String> addresses) {
      List<String> asList = new ArrayList<>(new HashSet<>(addresses));
      this.seedNodes = getSeedNodes(ConnectionString.fromHostnames(asList));
      return this;
    }

    /**
     * Sets the addresses of the Couchbase Server nodes to bootstrap against.
     * <p>
     * If a port is specified, it must be the KV service port.
     * <p>
     * The port may be omitted if Couchbase is listening on the standard KV ports
     * (11210 and 11207 for insecure and TLS connections, respectively).
     *
     * @param addresses seed nodes.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder seedNodes(final String... addresses) {
      return seedNodes(Arrays.asList(addresses));
    }

    /**
     * @deprecated use {@link #seedNodes(Collection)} instead.
     */
    @Deprecated
    public Builder hostnames(final List<String> addresses) {
      return seedNodes(addresses);
    }

    /**
     * @deprecated use {@link #seedNodes(String...)} instead.
     */
    @Deprecated
    public Builder hostnames(String... addresses) {
      return seedNodes(addresses);
    }

    /**
     * Connection string to bootstrap with.
     * <p>
     * Note: it overrides list of addresses defined by {@link #seedNodes(Collection)}.
     * <p>
     * Connection string specification defined in SDK-RFC-11:
     * https://github.com/couchbaselabs/sdk-rfcs/blob/master/rfc/0011-connection-string.md
     *
     * @param connectionString seed nodes.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder connectionString(String connectionString) {
      this.seedNodes = getSeedNodes(ConnectionString.create(connectionString));
      return this;
    }

    private static List<HostAndPort> getSeedNodes(ConnectionString cs) {
      return cs.hosts().stream()
          .map(s -> new HostAndPort(s.hostname(), s.port()))
          .collect(toList());
    }

    /**
     * Network selection strategy for connecting to clusters whose nodes have alternate hostnames.
     * This usually only matters if Couchbase is running in a containerized environment
     * and you're connecting from outside that environment.
     * <p>
     * Defaults to {@link NetworkResolution#AUTO} which attempts to infer the correct network name
     * by comparing the hostnames reported by Couchbase against the hostnames used to connect to the cluster.
     */
    public Builder networkResolution(NetworkResolution nr) {
      this.networkResolution = requireNonNull(nr);
      return this;
    }

    /**
     * Sets a custom event loop group.
     * <p>
     * If more than one client is initialized and runs at the same time,
     * you may see better performance if you create a single event loop group
     * for the clients to share.
     *
     * @param eventLoopGroup the group that should be used.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
      this.eventLoopGroup = eventLoopGroup;
      return this;
    }

    /**
     * The name of the bucket to use.
     *
     * @param bucket name of the bucket
     * @return this {@link Builder} for nice chainability.
     */
    public Builder bucket(final String bucket) {
      this.bucket = bucket;
      return this;
    }

    /**
     * Controls whether the client operates in collections-aware mode (defaults to false).
     * <p>
     * A client that is not collections-aware only receives events from the default collection.
     * <p>
     * A collections-aware client receives events from all collections (or the subset specified by
     * the collections/scope filter, configured separately). In this mode, users of the low-level API
     * are responsible for handling "DCP System Event" and "DCP Seqno Advanced" events in their
     * {@link ControlEventHandler}. Users of the high-level API should implement
     * {@link DatabaseChangeListener#onSeqnoAdvanced}.
     * <p>
     * It is fine to enable collections awareness even when connecting to a server that does
     * not support collections, as long as no collections/scope filter is configured.
     *
     * @return this {@link Builder} for nice chainability.
     * @see #scopeId(long)
     * @see #scopeName(String)
     * @see #collectionIds(Collection)
     * @see #collectionNames(Collection)
     */
    public Builder collectionsAware(boolean enable) {
      this.collectionsAware = enable;
      return this;
    }

    /**
     * Enables fine-grained trace logging to the
     * "com.couchbase.client.dcp.trace" category.
     *
     * @param level level at which to log the trace messages
     * @param documentIdIsInteresting (nullable) tests a document ID and returns true
     * if events related to this document ID should be logged. Null means log events for all documents.
     */
    public Builder trace(LogLevel level, Predicate<String> documentIdIsInteresting) {
      this.tracer = level == NONE ? Tracer.NOOP : new LoggingTracer(level, documentIdIsInteresting);
      return this;
    }

    public Builder collectionNames(Collection<String> qualifiedCollectionNames) {
      for (String name : qualifiedCollectionNames) {
        if (name == null) {
          throw new IllegalArgumentException("Collection name must not be null");
        }
        if (name.split("\\.", -1).length != 2) {
          throw new IllegalArgumentException("Collection name '" + name + "'" +
              " must be qualified by a scope name, like: myScope.myCollection");
        }
      }

      this.collectionNames = new HashSet<>(qualifiedCollectionNames);
      return this;
    }

    public Builder collectionNames(String... qualifiedCollectionNames) {
      return collectionNames(Arrays.asList(qualifiedCollectionNames));
    }

    /**
     * Configures the client to stream only from the collections identified by the given IDs.
     *
     * @param collectionIds IDs of the collections to stream.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder collectionIds(Collection<Long> collectionIds) {
      if (collectionIds.stream().anyMatch(Objects::isNull)) {
        throw new IllegalArgumentException("Collection ID must not be null");
      }
      this.collectionIds = new HashSet<>(collectionIds);
      return this;
    }

    /**
     * Configures the client to stream only from the collections identified by the given IDs.
     *
     * @param collectionIds IDs of the collections to stream.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder collectionIds(long... collectionIds) {
      return collectionIds(Arrays.stream(collectionIds).boxed().collect(toList()));
    }

    /**
     * Configures the client to stream only from the scope identified by the given ID.
     *
     * @param scopeName name of the scope to stream (may be null, in which case this method has no effect)
     * @return this {@link Builder} for nice chainability.
     */
    public Builder scopeName(String scopeName) {
      this.scopeName = isNullOrEmpty(scopeName) ? Optional.empty() : Optional.of(scopeName);
      return this;
    }

    /**
     * Configures the client to stream only from the scope identified by the given ID.
     *
     * @param scopeId IDs of the scope to stream.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder scopeId(long scopeId) {
      this.scopeId = OptionalLong.of(scopeId);
      return this;
    }

    public Builder credentials(String username, String password) {
      return credentialsProvider(new StaticCredentialsProvider(username, password));
    }

    public Builder credentialsProvider(final CredentialsProvider credentialsProvider) {
      return authenticator(new PasswordAuthenticator(credentialsProvider));
    }

    public Builder authenticator(Authenticator authenticator) {
      this.authenticator = requireNonNull(authenticator);
      return this;
    }

    /**
     * Sets the product information to include in the DCP client's User Agent
     * string. The User Agent will be part of the DCP connection name,
     * which appears in the Couchbase Server logs for log entries associated
     * with this client.
     * <p>
     * The product name may consist of alpha-numeric ASCII characters, plus
     * the following special characters: <code>!#$%&'*+-.^_`|~</code>
     * <p>
     * Invalid characters will be converted to underscores.
     * <p>
     * Comments may optionally convey additional context, such as the name of
     * the bucket being streamed or some other information about the client.
     *
     * @return this {@link Builder} for nice chainability.
     */
    public Builder userAgent(String productName, String productVersion, String... comments) {
      return connectionNameGenerator(DefaultConnectionNameGenerator.forProduct(productName, productVersion, comments));
    }

    /**
     * If specific names for DCP connections should be generated, a custom one can be provided.
     *
     * @param connectionNameGenerator custom generator.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder connectionNameGenerator(final ConnectionNameGenerator connectionNameGenerator) {
      this.connectionNameGenerator = connectionNameGenerator;
      return this;
    }

    /**
     * Set all kinds of DCP control params - check their description for more information.
     *
     * @param name the name of the param
     * @param value the value of the param
     * @return this {@link Builder} for nice chainability.
     */
    public Builder controlParam(final DcpControl.Names name, Object value) {
      this.dcpControl.put(name, value.toString());
      return this;
    }

    /**
     * Sets the compression mode for message values sent by Couchbase Server.
     * If not specified, defaults to {@link CompressionMode#ENABLED}.
     */
    public Builder compression(CompressionMode compressionMode) {
      this.dcpControl.compression(compressionMode);
      return this;
    }

    /**
     * If buffer pooling should be enabled (yes by default).
     *
     * @param pool enable or disable buffer pooling.
     * @return this {@link Builder} for nice chainability.
     */
    public Builder poolBuffers(final boolean pool) {
      this.poolBuffers = pool;
      return this;
    }

    /**
     * Sets a custom socket connect timeout.
     *
     * @param socketConnectTimeout the socket connect timeout in milliseconds.
     */
    public Builder socketConnectTimeout(long socketConnectTimeout) {
      this.socketConnectTimeout = socketConnectTimeout;
      return this;
    }

    /**
     * Time to wait for first configuration during bootstrap.
     */
    public Builder bootstrapTimeout(Duration bootstrapTimeout) {
      this.bootstrapTimeout = bootstrapTimeout;
      return this;
    }

    /**
     * When connecting to versions of Couchbase Server older than 5.5,
     * the DCP client polls the server for cluster topology config changes
     * at the specified interval.
     * <p>
     * When connecting to modern versions of Couchbase Server,
     * calling this method has no effect.
     *
     * @param configRefreshInterval time between config refresh requests.
     * Must be between 1 second and 2 minutes (inclusive).
     */
    public Builder configRefreshInterval(Duration configRefreshInterval) {
      if (configRefreshInterval.compareTo(Duration.ofSeconds(1)) < 0) {
        throw new IllegalArgumentException("Minimum config refresh interval is 1 second.");
      }
      if (configRefreshInterval.compareTo(Duration.ofMinutes(2)) > 0) {
        throw new IllegalArgumentException("Maximum config refresh interval is 2 minutes.");
      }
      this.configRefreshInterval = requireNonNull(configRefreshInterval);
      return this;
    }

    /**
     * The maximum number of reconnect attempts for DCP channels
     *
     * @param dcpChannelsReconnectMaxAttempts
     */
    public Builder dcpChannelsReconnectMaxAttempts(int dcpChannelsReconnectMaxAttempts) {
      this.dcpChannelsReconnectMaxAttempts = dcpChannelsReconnectMaxAttempts;
      return this;
    }

    /**
     * Delay between retry attempts for DCP channels
     *
     * @deprecated Doesn't do anything.
     */
    @Deprecated
    public Builder dcpChannelsReconnectDelay(Delay ignored) {
      return this;
    }

    /**
     * Sets the event bus to an alternative implementation.
     * <p>
     * This setting should only be tweaked in advanced cases.
     */
    public Builder eventBus(final EventBus eventBus) {
      this.eventBus = eventBus;
      return this;
    }

    /**
     * Sets the TLS configutation options
     */
    public Builder securityConfig(final SecurityConfig securityConfig) {
      this.securityConfig = requireNonNull(securityConfig);
      return this;
    }

    /**
     * Sets the TLS configutation options (from a builder, for convenience)
     */
    public Builder securityConfig(final SecurityConfig.Builder securityConfigBuilder) {
      return securityConfig(securityConfigBuilder.build());
    }

    /**
     * Enables rollback mitigation with the specified persistence polling interval.
     * Must be accompanied by a call to {@link #flowControl(int)}.
     * <p>
     * When rollback mitigation is enabled, stream events will not be propagated to the
     * {@link DataEventHandler} or {@link ControlEventHandler} until the event has been
     * persisted to disk on the active and all replica nodes.
     * <p>
     * To observe persistence, the client will poll each node at the given interval.
     * </p>
     * If a partition instance becomes unavailable, event propagation will pause until
     * the cluster stabilizes. Because recovery may take a long time, flow control
     * is required so the client need not buffer an indeterminately large number of events.
     */
    public Builder mitigateRollbacks(long persistencePollingInterval, TimeUnit unit) {
      this.persistencePollingIntervalMillis = unit.toMillis(persistencePollingInterval);
      return this;
    }

    /**
     * Enables <a href="https://github.com/couchbaselabs/dcp-documentation/blob/master/documentation/flow-control.md">
     * flow control</a> with the specified buffer size (in bytes) and a reasonable
     * default buffer ACK threshold. To specify a custom threshold, chain this method with
     * {@link #bufferAckWatermark(int)}.
     *
     * @param bufferSizeInBytes The amount of data the server will send before requiring an ACK
     * @see ChannelFlowController
     */
    public Builder flowControl(int bufferSizeInBytes) {
      controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, bufferSizeInBytes);
      if (bufferAckWatermark == 0) {
        bufferAckWatermark = 80;
      }
      return this;
    }

    /**
     * Specifies the registry the client should use when tracking metrics.
     * If not called, defaults to Micrometer's global registry.
     */
    public Builder meterRegistry(MeterRegistry meterRegistry) {
      this.meterRegistry = requireNonNull(meterRegistry);
      return this;
    }

    /**
     * Create the client instance ready to use.
     *
     * @return the built client instance.
     */
    public Client build() {
      if (authenticator == null) {
        throw new IllegalStateException("Must provide authenticator. Simplest way is to call credentials(username, password).");
      }

      if (authenticator.requiresTls() && !securityConfig.tlsEnabled()) {
        throw new IllegalStateException("The provided authenticator requires TLS, but TLS was not enabled in the SecurityConfig");
      }

      if (collectionsAware && !dcpControl.noopEnabled()) {
        throw new IllegalStateException("Collections awareness requires NOOPs; must not disable NOOPs.");
      }

      // it's fine to specify both collection UIDs and names, but there can be only one scope, so...
      if (scopeName.isPresent() && scopeId.isPresent()) {
        throw new IllegalStateException("May specify scope name or ID, but not both.");
      }

      final boolean hasCollections = !collectionIds.isEmpty() || !collectionNames.isEmpty();
      final boolean hasScope = scopeId.isPresent() || scopeName.isPresent();

      if (hasCollections && hasScope) {
        throw new IllegalStateException("May specify scope or collections, but not both.");
      }

      if ((hasCollections || hasScope) && !collectionsAware) {
        throw new IllegalStateException("Must call collectionsAware(true) when specifying scope or collections.");
      }

      return new Client(this);
    }
  }

  /**
   * <pre>
   *             _._                           _._
   *            ||||                           ||||
   *            ||||_           ___           _||||
   *            |  ||        .-'___`-.        ||  |
   *            \   /      .' .'_ _'. '.      \   /
   *            /~~|       | (| b d |) |       |~~\
   *           /'  |       |  |  '  |  |       |  `\
   * ,        /__.-:      ,|  | `-' |  |,      :-.__\       ,
   * |'-------(    \-''""/.|  /\___/\  |.\""''-/    )------'|
   * |         \_.-'\   /   '-._____.-'   \   /'-._/        |
   * |.---------\   /'._| _    .---. ===  |_.'\   /--------.|
   * '           \ /  | |\_\ _ \=v=/  _   | |  \ /          '
   *              `.  | | \_\_\ ~~~  (_)  | |  .'
   *                `'"'|`'--.__.^.__.--'`|'"'`
   *                    \                 /
   *                     `,..---'"'---..,'
   *                       :--..___..--:    TO INFINITY...
   *                        \         /
   *                        |`.     .'|       AND BEYOND!
   *                        |  :___:  |
   *                        |   | |   |
   *                        |   | |   |
   *                        |.-.| |.-.|
   *                        |`-'| |`-'|
   *                        |   | |   |
   *                       /    | |    \
   *                      |_____| |_____|
   *                      ':---:-'-:---:'
   *                      /    |   |    \
   *                 jgs /.---.|   |.---.\
   *                     `.____;   :____.'
   * </pre>
   */
  private static void buzzMe() {
    LOGGER.debug("To Infinity... AND BEYOND!");
  }

  /**
   * The {@link Environment} is responsible to carry various configuration and
   * state information throughout the lifecycle.
   */
  public static class Environment {
    private static final Logger log = LoggerFactory.getLogger(Environment.class);

    public static final Duration DEFAULT_BOOTSTRAP_TIMEOUT = Duration.ofSeconds(5);
    public static final Duration DEFAULT_CONFIG_REFRESH_INTERVAL = Duration.ofSeconds(2);
    public static final long DEFAULT_SOCKET_CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(1);
    public static final Retry DEFAULT_DCP_CHANNELS_RECONNECT_DELAY = Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(200));
    public static final int DEFAULT_DCP_CHANNELS_RECONNECT_MAX_ATTEMPTS = Integer.MAX_VALUE;
    private static final int DEFAULT_KV_PORT = 11210;
    private static final int DEFAULT_KV_TLS_PORT = 11207;

    private final List<HostAndPort> seedNodes;
    private final NetworkResolution networkResolution;
    private final ConnectionNameGenerator connectionNameGenerator;
    private final String bucket;
    private final boolean collectionsAware;
    private final OptionalLong scopeId;
    private final Optional<String> scopeName;
    private final Set<Long> collectionIds;
    private final Set<String> collectionNames;
    private final Authenticator authenticator;
    private final Duration bootstrapTimeout;
    private final Duration configRefreshInterval;
    private final DcpControl dcpControl;
    private final Set<OpenConnectionFlag> connectionFlags;
    private final EventLoopGroup eventLoopGroup;
    private final boolean eventLoopGroupIsPrivate;
    private final boolean poolBuffers;
    private final int bufferAckWatermark;
    private final long socketConnectTimeout;
    private final long persistencePollingIntervalMillis;
    private volatile DataEventHandler dataEventHandler;
    private volatile ControlEventHandler controlEventHandler;
    private final PersistedSeqnos persistedSeqnos = PersistedSeqnos.uninitialized();
    private final Retry dcpChannelsReconnectDelay;
    private final int dcpChannelsReconnectMaxAttempts;

    private final EventBus eventBus;
    private final Scheduler scheduler;
    private Disposable systemEventSubscription;
    private final SecurityConfig securityConfig;
    private final Tracer tracer;

    /**
     * Creates a new environment based on the builder.
     *
     * @param builder the builder to build the environment.
     */
    private Environment(final Client.Builder builder) {
      connectionNameGenerator = builder.connectionNameGenerator;
      bucket = builder.bucket;
      authenticator = builder.authenticator;
      bootstrapTimeout = builder.bootstrapTimeout;
      configRefreshInterval = builder.configRefreshInterval;
      dcpControl = builder.dcpControl;
      connectionFlags = unmodifiableSet(EnumSet.copyOf(builder.connectionFlags));
      eventLoopGroup = Optional.ofNullable(builder.eventLoopGroup)
          .orElseGet(Client::newEventLoopGroup);
      eventLoopGroupIsPrivate = builder.eventLoopGroup == null;
      bufferAckWatermark = builder.bufferAckWatermark;
      poolBuffers = builder.poolBuffers;
      socketConnectTimeout = builder.socketConnectTimeout;
      dcpChannelsReconnectDelay = builder.dcpChannelsReconnectDelay;
      dcpChannelsReconnectMaxAttempts = builder.dcpChannelsReconnectMaxAttempts;
      collectionsAware = builder.collectionsAware;
      collectionIds = Collections.unmodifiableSet(builder.collectionIds);
      collectionNames = Collections.unmodifiableSet(builder.collectionNames);
      scopeId = builder.scopeId;
      scopeName = builder.scopeName;
      this.tracer = builder.tracer;
      if (builder.eventBus != null) {
        eventBus = builder.eventBus;
        this.scheduler = null;
      } else {
        this.scheduler = Schedulers.parallel();
        eventBus = new DefaultDcpEventBus(scheduler);
      }
      securityConfig = builder.securityConfig;
      seedNodes = makeDefaultPortsExplicit(builder.seedNodes, securityConfig.tlsEnabled());
      networkResolution = builder.networkResolution;
      persistencePollingIntervalMillis = builder.persistencePollingIntervalMillis;

      if (persistencePollingIntervalMillis > 0) {
        if (bufferAckWatermark == 0) {
          throw new IllegalArgumentException("Rollback mitigation requires flow control.");
        }

        final StreamEventBuffer buffer = new StreamEventBuffer(eventBus);
        dataEventHandler = buffer;
        controlEventHandler = buffer;
      }
    }

    /**
     * Lists the bootstrap nodes.
     */
    public List<HostAndPort> clusterAt() {
      return seedNodes;
    }

    /**
     * Returns the configured hostname selection strategy.
     */
    public NetworkResolution networkResolution() {
      return networkResolution;
    }

    /**
     * Returns the currently attached data event handler.
     */
    public DataEventHandler dataEventHandler() {
      return dataEventHandler;
    }

    /**
     * Returns the stream event buffer used for rollback mitigation.
     *
     * @throws IllegalStateException if persistence polling / rollback mitigation is disabled
     */
    public StreamEventBuffer streamEventBuffer() {
      try {
        return (StreamEventBuffer) dataEventHandler;
      } catch (ClassCastException e) {
        throw new IllegalStateException("Stream event buffer not configured");
      }
    }

    /**
     * Returns the bookkeeper for observed seqno persistence.
     */
    public PersistedSeqnos persistedSeqnos() {
      return persistedSeqnos;
    }

    /**
     * Returns the interval between observeSeqno requests.
     * Values <= 0 disable persistence polling.
     */
    public long persistencePollingIntervalMillis() {
      return persistencePollingIntervalMillis;
    }

    /**
     * Returns true if and only if rollback mitigation / persistence polling is enabled.
     */
    public boolean persistencePollingEnabled() {
      return persistencePollingIntervalMillis > 0;
    }

    /**
     * Returns the current attached control event handler.
     */
    public ControlEventHandler controlEventHandler() {
      return controlEventHandler;
    }

    /**
     * Returns the name generator used to identify DCP sockets.
     */
    public ConnectionNameGenerator connectionNameGenerator() {
      return connectionNameGenerator;
    }

    /**
     * Name of the bucket used.
     */
    public String bucket() {
      return bucket;
    }

    /**
     * Whether the client should operate in collections-aware mode
     */
    public boolean collectionsAware() {
      return collectionsAware;
    }

    /**
     * Collection IDs to filter on, or empty for all collections.
     */
    public Set<Long> collectionIds() {
      return collectionIds;
    }

    /**
     * Collection names to filter on, or empty for all collections.
     */
    public Set<String> collectionNames() {
      return collectionNames;
    }

    /**
     * Scope to filter on, or empty to filter by collection IDs.
     */
    public OptionalLong scopeId() {
      return scopeId;
    }

    /**
     * Scope to filter on, or empty to filter by collection IDs.
     */
    public Optional<String> scopeName() {
      return scopeName;
    }

    /**
     * The authenticator for the connection
     */
    public Authenticator authenticator() {
      return authenticator;
    }

    /**
     * Returns all DCP control params set, may be empty.
     */
    public DcpControl dcpControl() {
      return dcpControl;
    }

    /**
     * Returns the flags to use when opening a DCP connection.
     */
    public Set<OpenConnectionFlag> connectionFlags() {
      return connectionFlags;
    }

    /**
     * The watermark in percent for buffer acknowledgements.
     */
    public int bufferAckWatermark() {
      return bufferAckWatermark;
    }

    /**
     * Returns the currently attached event loop group for IO process.ing.
     */
    public EventLoopGroup eventLoopGroup() {
      return eventLoopGroup;
    }

    /**
     * Polling interval when server does not support clustermap change notifications.
     */
    public Duration configRefreshInterval() {
      return configRefreshInterval;
    }

    /**
     * Time in milliseconds to wait for first configuration during bootstrap.
     */
    public Duration bootstrapTimeout() {
      return bootstrapTimeout;
    }

    /**
     * Set/Override the data event handler.
     */
    public void setDataEventHandler(DataEventHandler dataEventHandler) {
      if (persistencePollingEnabled()) {
        streamEventBuffer().setDataEventHandler(dataEventHandler);
      } else {
        this.dataEventHandler = dataEventHandler;
      }
    }

    /**
     * Set/Override the control event handler.
     */
    public void setControlEventHandler(ControlEventHandler controlEventHandler) {
      if (persistencePollingEnabled()) {
        streamEventBuffer().setControlEventHandler(controlEventHandler);
      } else {
        this.controlEventHandler = controlEventHandler;
      }
    }

    /**
     * Set/Override the control event handler.
     */
    public void setSystemEventHandler(final SystemEventHandler systemEventHandler) {
      if (systemEventSubscription != null) {
        systemEventSubscription.dispose();
      }
      if (systemEventHandler != null) {
        systemEventSubscription = eventBus().get()
            .filter(evt -> evt.type().equals(EventType.SYSTEM))
            .subscribe(systemEventHandler::onEvent);
      }
    }

    /**
     * If buffer pooling is enabled.
     */
    public boolean poolBuffers() {
      return poolBuffers;
    }

    /**
     * Socket connect timeout in milliseconds.
     */
    public long socketConnectTimeout() {
      return socketConnectTimeout;
    }

    /**
     * Returns the event bus where events are broadcasted on and can be published to.
     */
    public EventBus eventBus() {
      return eventBus;
    }

    /**
     * Returns the TLS configuration
     */
    public SecurityConfig securityConfig() {
      return securityConfig;
    }

    public Tracer tracer() {
      return tracer;
    }

    private static List<HostAndPort> makeDefaultPortsExplicit(List<HostAndPort> addresses, boolean sslEnabled) {
      final int defaultKvPort = sslEnabled ? DEFAULT_KV_TLS_PORT : DEFAULT_KV_PORT;
      final List<HostAndPort> result = new ArrayList<>();

      for (HostAndPort node : addresses) {
        if (node.port() == 8091 || node.port() == 18091) {
          log.warn("Seed node '{}' uses port '{}' which is likely incorrect." +
                  " This should be the port of the KV service, not the Manager service." +
                  " If the connection fails, omit the port so the client can supply the correct default.",
              node.host(), node.port());
        }

        result.add(node.port() == 0 ? node.withPort(defaultKvPort) : node);
      }

      return result;
    }

    /**
     * Shut down this stateful environment.
     * <p>
     * Note that it will only release/terminate resources which are owned by the client,
     * especially if a custom event loop group is passed in it needs to be shut down
     * separately.
     *
     * @return a {@link Mono} indicating completion of the shutdown process.
     */
    public Mono<Void> shutdown() {
      Mono<Boolean> loopShutdown = Mono.empty();

      if (eventLoopGroupIsPrivate) {
        loopShutdown = Mono.create(sink ->
            eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS)
                .addListener(future -> {
                  if (future.isSuccess()) {
                    sink.success();
                  } else {
                    sink.error(future.cause());
                  }
                }));
      }

      return loopShutdown
          .then();
    }

    @Override
    public String toString() {
      return "ClientEnvironment{" +
          "seedNodes=" + seedNodes +
          ", connectionNameGenerator=" + connectionNameGenerator +
          ", bucket='" + bucket + '\'' +
          ", collectionsAware=" + collectionsAware +
          ", collectionIds=" + collectionIds +
          ", collectionNames=" + collectionNames +
          ", scopeId=" + scopeId +
          ", scopeName=" + scopeName +
          ", dcpControl=" + dcpControl +
          ", eventLoopGroup=" + eventLoopGroup.getClass().getSimpleName() +
          ", eventLoopGroupIsPrivate=" + eventLoopGroupIsPrivate +
          ", poolBuffers=" + poolBuffers +
          ", bufferAckWatermark=" + bufferAckWatermark +
          ", bootstrapTimeout=" + bootstrapTimeout +
          ", configRefreshInterval=" + configRefreshInterval +
          ", securityConfig=" + securityConfig.exportAsMap() +
          '}';
    }

    public int dcpChannelsReconnectMaxAttempts() {
      return dcpChannelsReconnectMaxAttempts;
    }
  }
}
