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

import com.couchbase.client.core.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.core.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.core.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.core.deps.io.netty.util.AttributeKey;
import com.couchbase.client.core.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.core.deps.io.netty.util.concurrent.GenericFutureListener;
import com.couchbase.client.core.deps.io.netty.util.concurrent.ImmediateEventExecutor;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.buffer.DcpOps;
import com.couchbase.client.dcp.core.state.AbstractStateMachine;
import com.couchbase.client.dcp.core.state.LifecycleState;
import com.couchbase.client.dcp.core.state.NotConnectedException;
import com.couchbase.client.dcp.core.time.Delay;
import com.couchbase.client.dcp.core.utils.DefaultObjectMapper;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.highlevel.StreamOffset;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.message.DcpCloseStreamRequest;
import com.couchbase.client.dcp.message.DcpFailoverLogRequest;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosRequest;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.PartitionAndSeqno;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.message.StreamFlag;
import com.couchbase.client.dcp.message.VbucketState;
import com.couchbase.client.dcp.metrics.DcpChannelMetrics;
import com.couchbase.client.dcp.metrics.DcpClientMetrics;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpMessageHandler;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.dcp.transport.netty.DcpResponse;
import com.couchbase.client.dcp.transport.netty.DcpResponseListener;
import com.couchbase.client.dcp.util.AdaptiveDelay;
import com.couchbase.client.dcp.util.AtomicBooleanArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil.safeRelease;
import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.core.logging.RedactableArgument.user;
import static com.couchbase.client.dcp.message.HelloFeature.COLLECTIONS;
import static com.couchbase.client.dcp.message.MessageUtil.GET_COLLECTIONS_MANIFEST_OPCODE;
import static com.couchbase.client.dcp.message.ResponseStatus.KEY_EXISTS;
import static com.couchbase.client.dcp.message.ResponseStatus.NOT_MY_VBUCKET;
import static com.couchbase.client.dcp.message.ResponseStatus.ROLLBACK_REQUIRED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Logical representation of a DCP cluster connection.
 * <p>
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel extends AbstractStateMachine<LifecycleState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DcpChannel.class);

  private final DcpChannelControlHandler controlHandler;
  private volatile boolean isShutdown;
  private volatile Channel channel;
  private volatile ChannelFuture connectFuture;
  private final DcpChannelMetrics metrics;
  private final DcpClientMetrics clientMetrics;

  /**
   * The original host and port used to created the channel.
   * Absolutely certain not to have been the victim of a reverse DNS lookup.
   * <p>
   * (It's not clear whether Channel.remoteAddress() has the same guarantee.
   * This seems like something that could vary by channel implementation.)
   */
  private static final AttributeKey<HostAndPort> HOST_AND_PORT = AttributeKey.valueOf("hostAndPort");

  /**
   * Limits how frequently the client may attempt to reconnect after a successful connection.
   * Prevents tight reconnect loops if the connection is immediately closed by
   * {@link com.couchbase.client.dcp.buffer.PersistencePollingHandler}.
   */
  private final AdaptiveDelay reconnectDelay = new AdaptiveDelay(
      Delay.exponential(TimeUnit.MILLISECONDS, 4096, 32),
      Duration.ofSeconds(10));

  final Client.Environment env;
  final HostAndPort address;
  final AtomicBooleanArray streamIsOpen = new AtomicBooleanArray(1024);
  final Conductor conductor;

  public DcpChannel(HostAndPort address, final Client.Environment env, final Conductor conductor, DcpClientMetrics clientMetrics) {
    super(LifecycleState.DISCONNECTED);
    this.address = address;
    this.env = env;
    this.conductor = conductor;
    this.controlHandler = new DcpChannelControlHandler(this);
    this.isShutdown = false;
    this.clientMetrics = requireNonNull(clientMetrics);
    this.metrics = clientMetrics.channelMetrics(address);
  }

  public static HostAndPort getHostAndPort(Channel channel) {
    return channel.attr(HOST_AND_PORT).get();
  }

  /**
   * @see DcpMessageHandler#sendRequest(ByteBuf)
   */
  public Future<DcpResponse> sendRequest(ByteBuf message) {
    if (channel == null) {
      safeRelease(message);
      return ImmediateEventExecutor.INSTANCE.newFailedFuture(
          new NotConnectedException("Failed to issue request; channel is not active."));
    }

    return channel.pipeline().get(DcpMessageHandler.class).sendRequest(message);
  }

  private Bootstrap newBootstrap() {
    ByteBufAllocator allocator = env.poolBuffers()
        ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
    return new Bootstrap()
        .option(ChannelOption.ALLOCATOR, allocator)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) env.socketConnectTimeout())
        .remoteAddress(address.host(), address.port())
        .attr(HOST_AND_PORT, address) // stash it away separately for safety (paranoia?)
        .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
        .handler(new DcpPipeline(env, controlHandler, conductor.bucketConfigArbiter(), metrics, clientMetrics))
        .group(env.eventLoopGroup());
  }

  public Mono<Void> connect() {
    return Mono.create(sink -> {
      if (isShutdown || state() != LifecycleState.DISCONNECTED) {
        sink.success();
        return;
      }

      final Bootstrap bootstrap = newBootstrap();
      transitionState(LifecycleState.CONNECTING);
      connectFuture = metrics.trackConnect(bootstrap.connect());
      connectFuture.addListener(new GenericFutureListener<ChannelFuture>() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (!future.isSuccess()) {
            LOGGER.info("Connect attempt to {} failed.", system(address), future.cause());
            transitionState(LifecycleState.DISCONNECTED);
            sink.error(future.cause());
            return;
          }

          channel = future.channel();
          metrics.trackDisconnect(channel.closeFuture());

          if (isShutdown) {
            LOGGER.info("Connected Node {}, but got instructed to disconnect in the meantime.", system(address));
            // isShutdown before we could finish the connect :/
            disconnect()
                .doOnSuccess(v -> sink.success())
                .onErrorResume(e -> {
                  LOGGER.warn("Got error during disconnect.", e);
                  sink.error(e);
                  return Mono.empty();
                })
                .subscribe();

            return;
          }

          transitionState(LifecycleState.CONNECTED);

          // This time get the address from the Netty channel because it includes resolved hostname.
          // Don't do this everywhere, since Channel.remoteAddress() returns null if channel is not connected.
          LOGGER.info("Connected to Node {}", system(channel.remoteAddress()));

          // attach callback which listens on future close and dispatches a
          // reconnect if needed.
          channel.closeFuture().addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              LOGGER.debug("Got notified of channel close on Node {}", address);

              // dispatchReconnect() may restart the stream from what it thinks is the
              // current state. Buffered events are not reflected in the session state, so
              // clear the stream buffer to prevent duplicate (potentially obsolete) events.
              // See also: DcpChannelControlHandler.filterDcpStreamEndMessage() which
              // does something similar.
              if (env.persistencePollingEnabled()) {
                for (int vbid = 0; vbid < streamIsOpen.length(); vbid++) {
                  if (streamIsOpen.get(vbid)) {
                    env.streamEventBuffer().clear(vbid);
                  }
                }
              }

              transitionState(LifecycleState.DISCONNECTED);
              if (!isShutdown) {
                dispatchReconnect();
              }
              channel = null;
            }
          });

          sink.success();
        }
      });
    });
  }

  private void dispatchReconnect() {
    if (isShutdown) {
      LOGGER.debug("Ignoring reconnect on {} because already shutdown.", address);
      return;
    }
    LOGGER.info("Node {} socket closed, initiating reconnect.", system(address));

    final Duration delay = reconnectDelay.calculate();
    if (delay.compareTo(Duration.ZERO) > 0) {
      LOGGER.info("Delaying reconnection attempt by {}ms", delay.toMillis());
    }

    // Always start with timer even if delay is zero; that way the scheduler executing the rest of the flow is the same
    // regardless of whether the reconnect attempt was delayed. One less thing to think about when debugging.
    Mono.delay(delay).then(
            connect()
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(32))
                    .maxBackoff(Duration.ofSeconds(4))
                    .doAfterRetry(retrySignal -> LOGGER.debug("Rescheduling Node reconnect for DCP channel {}", address))))
        .doOnError(e -> LOGGER.warn("Got error during connect (maybe retried) for node {}", system(address), e))
        .doOnSuccess(ignored -> {
          LOGGER.debug("Completed Node connect for DCP channel {}", address);
          for (int vbid = 0; vbid < streamIsOpen.length(); vbid++) {
            if (streamIsOpen.get(vbid)) {
              conductor.maybeMovePartition(vbid);
            }
          }
        }).subscribe();
  }

  public boolean isShutdown() {
    return isShutdown;
  }

  public Mono<Void> disconnect() {
    return Mono.create(sink -> {
      isShutdown = true;
      if (channel != null) {
        transitionState(LifecycleState.DISCONNECTING);
        final ChannelFuture closeFuture = metrics.trackDisconnect(channel.close());
        closeFuture.addListener(new GenericFutureListener<ChannelFuture>() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            transitionState(LifecycleState.DISCONNECTED);
            LOGGER.info("Disconnected from Node {}", system(address()));
            if (future.isSuccess()) {
              sink.success();
            } else {
              LOGGER.debug("Error during channel close.", future.cause());
              sink.error(future.cause());
            }
          }
        });
      } else if (connectFuture != null) {
        connectFuture.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture channelFuture) throws Exception {
            if (channelFuture.isSuccess()) {
              channelFuture.channel().closeFuture().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture closeFuture) throws Exception {
                  if (closeFuture.isSuccess()) {
                    sink.success();
                  } else {
                    sink.error(closeFuture.cause());
                  }
                }
              });
            } else {
              sink.success();
            }
          }
        });
      } else {
        sink.success();
      }
    });

  }

  public HostAndPort address() {
    return address;
  }

  /**
   * Returns empty optional if collections are not enabled for this channel,
   * otherwise the current collections manifest.
   */
  public Mono<Optional<CollectionsManifest>> getCollectionsManifest() {
    return Mono.create(sink -> {
      if (state() != LifecycleState.CONNECTED) {
        sink.error(new NotConnectedException());
        return;
      }

      if (!COLLECTIONS.isEnabled(channel)) {
        sink.success(Optional.empty());
        return;
      }

      ByteBuf buffer = Unpooled.buffer();
      MessageUtil.initRequest(GET_COLLECTIONS_MANIFEST_OPCODE, buffer);

      sendRequest(buffer).addListener(new DcpResponseListener() {
        @Override
        public void operationComplete(Future<DcpResponse> future) throws Exception {
          if (!future.isSuccess()) {

            if (future.cause() instanceof NotConnectedException) {
              LOGGER.debug("Failed to get collections manifest from {}; {}", address, future.cause().toString());
            } else {
              LOGGER.warn("Failed to get collections manifest from {}; {}", address, future.cause().toString());
            }
            sink.error(future.cause());
            return;
          }

          final DcpResponse dcpResponse = future.getNow();
          final ByteBuf buf = dcpResponse.buffer();
          try {
            final ResponseStatus status = dcpResponse.status();
            if (!status.isSuccess()) {
              LOGGER.warn("Failed to get collections manifest from {}, response status: {}", address, status);
              sink.error(new DcpOps.BadResponseStatusException(status));
              return;
            }

            byte[] manifestJsonBytes = MessageUtil.getContentAsByteArray(buf);
            LOGGER.debug("Got collections manifest from {} ; {}", address, new String(manifestJsonBytes, UTF_8));
            try {
              CollectionsManifest manifest = CollectionsManifest.fromJson(manifestJsonBytes);
              sink.success(Optional.of(manifest));
            } catch (Exception e) {
              LOGGER.error("Unparsable collections manifest from {} ; {}", system(address), user(new String(manifestJsonBytes, UTF_8)), e);
              sink.error(new RuntimeException("Failed to parse collections manifest", e));
            }

          } finally {
            buf.release();
          }
        }
      });
    });
  }

  public Mono<Void> openStream(
      final int vbid,
      final StreamOffset startOffset,
      final long endSeqno,
      final CollectionsManifest manifest,
      final Set<StreamFlag> flags
  ) {
    return Mono.create(sink -> {
      if (state() != LifecycleState.CONNECTED) {
        sink.error(new NotConnectedException());
        return;
      }

      final long startSeqno = startOffset.getSeqno();
      final long origSnapshotStartSeqno = startOffset.getSnapshot().getStartSeqno();
      final long origSnapshotEndSeqno = startOffset.getSnapshot().getEndSeqno();
      final long vbuuid = startOffset.getVbuuid();
      final long collectionsManifestuid = startOffset.getCollectionsManifestUid();

      final long snapshotStartSeqno;
      final long snapshotEndSeqno;

      if (origSnapshotStartSeqno == startSeqno + 1) {
        // startSeqno must be >= snapshotStartSeqno. If we get here, then we probably received
        // a snapshot marker and then disconnected before receiving the first seqno in the snapshot.
        // todo rework how PartitionState stores snapshot markers so we never get into this state.
        // One possibility would be to track the snapshot marker to assign to new events
        // separately from the stream offset's snapshot marker.
        LOGGER.debug("Disregarding snapshot marker from the future.");
        snapshotStartSeqno = startSeqno;
        snapshotEndSeqno = startSeqno;
      } else {
        snapshotEndSeqno = origSnapshotEndSeqno;
        snapshotStartSeqno = origSnapshotStartSeqno;
      }

      Set<StreamFlag> effectiveFlags = flags.stream()
          .filter(flag -> conductor.hasCapabilities(flag.requiredCapabilities()))
          .collect(toSet());

      Set<StreamFlag> unsupportedFlags = new HashSet<>(flags);
      unsupportedFlags.removeAll(effectiveFlags);
      if (!unsupportedFlags.isEmpty()) {
        LOGGER.debug("Ignoring unsupported optional stream flags: {}", unsupportedFlags);
      }

      LOGGER.debug("Opening Stream against {} with vbid: {}, vbuuid: {}, startSeqno: {}, " +
              "endSeqno: {},  snapshotStartSeqno: {}, snapshotEndSeqno: {}, manifest: {}, flags: {}",
          address, vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno, manifest, effectiveFlags);

      ByteBuf buffer = Unpooled.buffer();
      DcpOpenStreamRequest.init(buffer, effectiveFlags, vbid);
      DcpOpenStreamRequest.vbuuid(buffer, vbuuid);
      DcpOpenStreamRequest.startSeqno(buffer, startSeqno);
      DcpOpenStreamRequest.endSeqno(buffer, endSeqno);
      DcpOpenStreamRequest.snapshotStartSeqno(buffer, snapshotStartSeqno);
      DcpOpenStreamRequest.snapshotEndSeqno(buffer, snapshotEndSeqno);

      final Map<String, Object> value = new HashMap<>();

      if (COLLECTIONS.isEnabled(channel)) {
        final Set<Long> collectionIds = new HashSet<>(env.collectionIds());
        env.collectionNames().forEach(name -> {
          CollectionsManifest.CollectionInfo c = manifest.getCollection(name);
          if (c == null) {
            sink.error(new RuntimeException("Can't stream from collection '" + name + "' because it does not exist (not present in the collections manifest)."));
            return;
          }
          LOGGER.debug("resolved collection name '{}' to UID {}", name, c.id());
          collectionIds.add(c.id());
        });

        final OptionalLong scopeId;

        if (env.scopeName().isPresent()) {
          final String scopeName = env.scopeName().get();
          CollectionsManifest.ScopeInfo s = manifest.getScope(scopeName);
          if (s == null) {
            sink.error(new RuntimeException("Can't stream from scope '" + scopeName + "' because it does not exist (not present in the collections manifest)."));
            return;
          }
          LOGGER.debug("resolved scope name '{}' to UID {}", scopeName, s.id());
          scopeId = OptionalLong.of(s.id());
        } else {
          scopeId = env.scopeId();
        }

        // NOTE: this is the manifest UID from the stream offset, which may differ from the current manifest.
        value.put("uid", formatUid(collectionsManifestuid));

        if (!collectionIds.isEmpty()) {
          value.put("collections", formatUids(collectionIds));
        } else if (scopeId.isPresent()) {
          value.put("scope", formatUid(scopeId.getAsLong()));
        }

        try {
          byte[] bytes = DefaultObjectMapper.writeValueAsBytes(value);
          LOGGER.debug("opening stream for partition {} with value: {}", vbid, new String(bytes, UTF_8));
          MessageUtil.setContent(bytes, buffer);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      env.tracer().onStreamStart(address, vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno, value);

      sendRequest(buffer).addListener(new DcpResponseListener() {
        @Override
        public void operationComplete(Future<DcpResponse> future) throws Exception {
          if (!future.isSuccess()) {
            env.tracer().onStreamStartFailed(address, vbid, future.cause().toString());
            LOGGER.debug("Failed open Stream against {} with vbid: {}", address, vbid);
            streamIsOpen.set(vbid, false);
            sink.error(future.cause());
            return;
          }

          final DcpResponse dcpResponse = future.getNow();
          final ByteBuf buf = dcpResponse.buffer();
          try {
            final ResponseStatus status = dcpResponse.status();

            if (!status.isSuccess()) {
              env.tracer().onStreamStartFailed(address, vbid, status.formatted());
            }

            if (status == KEY_EXISTS) {
              LOGGER.debug("Stream already open against {} with vbid: {}", address, vbid);
              sink.success();
              return;
            }

            if (!status.isSuccess()) {
              LOGGER.debug("Failed open Stream against {} with vbid: {}", address, vbid);
              streamIsOpen.set(vbid, false);
            }

            if (status.isSuccess()) {
              LOGGER.debug("Opened Stream against {} with vbid: {}", address, vbid);
              streamIsOpen.set(vbid, true);

              sink.success();

              ByteBuf flog = Unpooled.buffer();
              DcpFailoverLogResponse.init(flog);
              DcpFailoverLogResponse.vbucket(flog, DcpOpenStreamResponse.vbucket(buf));
              ByteBuf content = MessageUtil.getContent(buf).copy().writeShort(vbid);
              MessageUtil.setContent(content, flog);
              content.release();
              env.controlEventHandler().onEvent(ChannelFlowController.dummy, flog);

            } else if (status == ROLLBACK_REQUIRED) {
              sink.error(new RollbackException());

              // create a rollback message and emit
              ByteBuf rb = Unpooled.buffer();
              RollbackMessage.init(rb, vbid, DcpOpenStreamResponse.rollbackSeqno(buf));
              env.controlEventHandler().onEvent(ChannelFlowController.dummy, rb);

            } else if (status == NOT_MY_VBUCKET) {
              sink.error(new NotMyVbucketException());

            } else {
              sink.error(new DcpOps.BadResponseStatusException(status));
            }
          } finally {
            buf.release();
          }
        }
      });
    });
  }

  private static String formatUid(long uid) {
    return Long.toHexString(uid);
  }

  private static List<String> formatUids(Collection<Long> uids) {
    return uids.stream()
        .map(DcpChannel::formatUid)
        .collect(toList());
  }

  public Mono<Void> closeStream(final int vbid) {
    return Mono.create(sink -> {
      if (state() != LifecycleState.CONNECTED) {
        sink.error(new NotConnectedException());
        return;
      }

      LOGGER.debug("Closing Stream against {} with vbid: {}", address, vbid);

      ByteBuf buffer = Unpooled.buffer();
      DcpCloseStreamRequest.init(buffer);
      DcpCloseStreamRequest.vbucket(buffer, vbid);

      sendRequest(buffer).addListener(new DcpResponseListener() {
        @Override
        public void operationComplete(Future<DcpResponse> future) throws Exception {
          streamIsOpen.set(vbid, false);
          if (future.isSuccess()) {
            future.getNow().buffer().release();
            LOGGER.debug("Closed Stream against {} with vbid: {}", address, vbid);
            sink.success();
          } else {
            LOGGER.debug("Failed close Stream against {} with vbid: {}", address, vbid);
            sink.error(future.cause());
          }
        }
      });
    });
  }

  /**
   * Returns all seqnos for all vbuckets on that channel.
   */
  public Mono<List<PartitionAndSeqno>> getSeqnos() {
    return Mono.create(sink -> {
      if (state() != LifecycleState.CONNECTED) {
        sink.error(new NotConnectedException());
        return;
      }

      ByteBuf buffer = Unpooled.buffer();
      DcpGetPartitionSeqnosRequest.init(buffer);
      DcpGetPartitionSeqnosRequest.vbucketState(buffer, VbucketState.ACTIVE);

      sendRequest(buffer).addListener(new DcpResponseListener() {
        @Override
        public void operationComplete(Future<DcpResponse> future) throws Exception {
          if (!future.isSuccess()) {
            sink.error(future.cause());
            return;
          }

          DcpResponse response = future.getNow();
          ByteBuf buf = response.buffer();
          try {
            if (!response.status().isSuccess()) {
              String content = MessageUtil.getContentAsString(response.buffer());
              String msg = "Failed to get seqnos. Details: " + content;
              sink.error(new DcpOps.BadResponseStatusException(response.status(), msg));
              return;
            }

            sink.success(DcpGetPartitionSeqnosResponse.parse(buf));
          } finally {
            buf.release();
          }
        }
      });
    });
  }

  public Mono<ByteBuf> getFailoverLog(final int vbid) {
    return Mono.create(sink -> {
      if (state() != LifecycleState.CONNECTED) {
        sink.error(new NotConnectedException());
        return;
      }

      ByteBuf buffer = Unpooled.buffer();
      DcpFailoverLogRequest.init(buffer);
      DcpFailoverLogRequest.vbucket(buffer, vbid);

      LOGGER.debug("Asked for failover log on {} for vbid: {}", address, vbid);
      sendRequest(buffer).addListener(new DcpResponseListener() {
        @Override
        public void operationComplete(Future<DcpResponse> future) throws Exception {
          if (!future.isSuccess()) {
            LOGGER.debug("Failed to ask for failover log on {} for vbid: {}", address, vbid);
            sink.error(future.cause());
            return;
          }
          ByteBuf buf = future.getNow().buffer();
          try {
            ByteBuf flog = Unpooled.buffer();
            DcpFailoverLogResponse.init(flog);
            DcpFailoverLogResponse.vbucket(flog, DcpFailoverLogResponse.vbucket(buf));
            ByteBuf copiedBuf = MessageUtil.getContent(buf).copy().writeShort(vbid);
            MessageUtil.setContent(copiedBuf, flog);
            copiedBuf.release();

            LOGGER.debug("Failover log for vbid {} is {}", vbid, DcpFailoverLogResponse.toString(flog));
            sink.success(flog);

          } finally {
            buf.release();
          }
        }
      });
    });
  }

  public boolean streamIsOpen(int vbid) {
    return streamIsOpen.get(vbid);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HostAndPort) {
      return address.equals(o);
    } else if (o instanceof DcpChannel) {
      return address.equals(((DcpChannel) o).address);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return address.hashCode();
  }

  @Override
  public String toString() {
    return "DcpChannel{address=" + address + '}';
  }
}
