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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.message.DcpCloseStreamRequest;
import com.couchbase.client.dcp.message.DcpFailoverLogRequest;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.message.VbucketState;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpMessageHandler;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.dcp.transport.netty.DcpResponse;
import com.couchbase.client.dcp.transport.netty.DcpResponseListener;
import com.couchbase.client.dcp.util.AdaptiveDelay;
import com.couchbase.client.dcp.util.AtomicBooleanArray;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelFutureListener;
import com.couchbase.client.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import com.couchbase.client.deps.io.netty.util.concurrent.ImmediateEventExecutor;
import rx.Completable;
import rx.CompletableSubscriber;
import rx.Single;
import rx.SingleSubscriber;
import rx.Subscription;
import rx.functions.Action4;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.message.ResponseStatus.KEY_EXISTS;
import static com.couchbase.client.dcp.message.ResponseStatus.NOT_MY_VBUCKET;
import static com.couchbase.client.dcp.message.ResponseStatus.ROLLBACK_REQUIRED;
import static com.couchbase.client.dcp.util.retry.RetryBuilder.any;
import static com.couchbase.client.deps.io.netty.util.ReferenceCountUtil.safeRelease;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Logical representation of a DCP cluster connection.
 * <p>
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel extends AbstractStateMachine<LifecycleState> {

  private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannel.class);

  /**
   * A "do nothing" flow controller for when an event does not require acknowledgement,
   * and the "real" flow controller isn't easily accessible.
   */
  private static final ChannelFlowController dummyFlowController = new ChannelFlowController() {
    @Override
    public void ack(ByteBuf message) {
    }

    @Override
    public void ack(int numBytes) {
    }
  };

  private final DcpChannelControlHandler controlHandler;
  private volatile boolean isShutdown;
  private volatile Channel channel;
  private volatile ChannelFuture connectFuture;

  /**
   * Limits how frequently the client may attempt to reconnect after a successful connection.
   * Prevents tight reconnect loops if the connection is immediately closed by
   * {@link com.couchbase.client.dcp.buffer.PersistencePollingHandler}.
   */
  private final AdaptiveDelay reconnectDelay = new AdaptiveDelay(
      Delay.exponential(TimeUnit.MILLISECONDS, 4096, 32),
      Duration.ofSeconds(10));

  final ClientEnvironment env;
  final InetSocketAddress inetAddress;
  final AtomicBooleanArray streamIsOpen = new AtomicBooleanArray(1024);
  final Conductor conductor;

  public DcpChannel(InetSocketAddress inetAddress, final ClientEnvironment env, final Conductor conductor) {
    super(LifecycleState.DISCONNECTED);
    this.inetAddress = inetAddress;
    this.env = env;
    this.conductor = conductor;
    this.controlHandler = new DcpChannelControlHandler(this);
    this.isShutdown = false;
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

  public Completable connect() {
    return Completable.create(new Completable.OnSubscribe() {
      @Override
      public void call(final CompletableSubscriber subscriber) {
        if (isShutdown || state() != LifecycleState.DISCONNECTED) {
          subscriber.onCompleted();
          return;
        }

        ByteBufAllocator allocator = env.poolBuffers()
            ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
        final Bootstrap bootstrap = new Bootstrap()
            .option(ChannelOption.ALLOCATOR, allocator)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) env.socketConnectTimeout())
            .remoteAddress(inetAddress)
            .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
            .handler(new DcpPipeline(env, controlHandler, conductor.configProvider()))
            .group(env.eventLoopGroup());

        transitionState(LifecycleState.CONNECTING);
        connectFuture = bootstrap.connect();
        connectFuture.addListener(new GenericFutureListener<ChannelFuture>() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              channel = future.channel();
              if (isShutdown) {
                LOGGER.info("Connected Node {}, but got instructed to disconnect in " +
                    "the meantime.", system(inetAddress));
                // isShutdown before we could finish the connect :/
                disconnect().subscribe(new CompletableSubscriber() {
                  @Override
                  public void onCompleted() {
                    subscriber.onCompleted();
                  }

                  @Override
                  public void onError(Throwable e) {
                    LOGGER.warn("Got error during disconnect.", e);
                  }

                  @Override
                  public void onSubscribe(Subscription d) {
                    // ignored.
                  }
                });
              } else {
                transitionState(LifecycleState.CONNECTED);
                LOGGER.info("Connected to Node {}", system(inetAddress));

                // attach callback which listens on future close and dispatches a
                // reconnect if needed.
                channel.closeFuture().addListener(new GenericFutureListener<ChannelFuture>() {
                  @Override
                  public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.debug("Got notified of channel close on Node {}", inetAddress);

                    // dispatchReconnect() may restart the stream from what it thinks is the
                    // current state. Buffered events are not reflected in the session state, so
                    // clear the stream buffer to prevent duplicate (potentially obsolete) events.
                    // See also: DcpChannelControlHandler.filterDcpStreamEndMessage() which
                    // does something similar.
                    if (env.persistencePollingEnabled()) {
                      for (short vbid = 0; vbid < streamIsOpen.length(); vbid++) {
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

                subscriber.onCompleted();
              }
            } else {
              LOGGER.info("Connect attempt to {} failed.", system(inetAddress), future.cause());
              transitionState(LifecycleState.DISCONNECTED);
              subscriber.onError(future.cause());
            }
          }
        });
      }
    });
  }

  private void dispatchReconnect() {
    if (isShutdown) {
      LOGGER.debug("Ignoring reconnect on {} because already shutdown.", inetAddress);
      return;
    }
    LOGGER.info("Node {} socket closed, initiating reconnect.", system(inetAddress));

    final long delayMillis = reconnectDelay.calculate().toMillis();
    if (delayMillis > 0) {
      LOGGER.info("Delaying reconnection attempt by {}ms", delayMillis);
    }

    // Always start with timer even if delay is zero; that way the scheduler executing the rest of the flow is the same
    // regardless of whether the reconnect attempt was delayed. One less thing to think about when debugging.
    Completable.timer(delayMillis, MILLISECONDS).andThen(
        connect().retryWhen(any().max(Integer.MAX_VALUE).delay(Delay.exponential(TimeUnit.MILLISECONDS, 4096, 32))
            .doOnRetry(new Action4<Integer, Throwable, Long, TimeUnit>() {
              @Override
              public void call(Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) {
                LOGGER.debug("Rescheduling Node reconnect for DCP channel {}", inetAddress);
              }
            }).build()))
        .subscribe(new CompletableSubscriber() {
          @Override
          public void onCompleted() {
            LOGGER.debug("Completed Node connect for DCP channel {}", inetAddress);
            for (short vbid = 0; vbid < streamIsOpen.length(); vbid++) {
              if (streamIsOpen.get(vbid)) {
                conductor.maybeMovePartition(vbid);
              }
            }
          }

          @Override
          public void onError(Throwable e) {
            LOGGER.warn("Got error during connect (maybe retried) for node {}", system(inetAddress), e);
          }

          @Override
          public void onSubscribe(Subscription d) {
            // ignored.
          }
        });
  }

  public boolean isShutdown() {
    return isShutdown;
  }

  public Completable disconnect() {
    return Completable.create(new Completable.OnSubscribe() {
      @Override
      public void call(final CompletableSubscriber subscriber) {
        isShutdown = true;
        if (channel != null) {
          transitionState(LifecycleState.DISCONNECTING);
          channel.close().addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              transitionState(LifecycleState.DISCONNECTED);
              LOGGER.info("Disconnected from Node {}", system(address()));
              if (future.isSuccess()) {
                subscriber.onCompleted();
              } else {
                LOGGER.debug("Error during channel close.", future.cause());
                subscriber.onError(future.cause());
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
                      subscriber.onCompleted();
                    } else {
                      subscriber.onError(closeFuture.cause());
                    }
                  }
                });
              } else {
                subscriber.onCompleted();
              }
            }
          });
        } else {
          subscriber.onCompleted();
        }
      }
    });

  }

  public InetSocketAddress address() {
    return inetAddress;
  }

  public Completable openStream(final short vbid, final long vbuuid, final long startSeqno, final long endSeqno,
                                final long origSnapshotStartSeqno, final long origSnapshotEndSeqno) {
    return Completable.create(new Completable.OnSubscribe() {
      @Override
      public void call(final CompletableSubscriber subscriber) {
        if (state() != LifecycleState.CONNECTED) {
          subscriber.onError(new NotConnectedException());
          return;
        }

        final long snapshotStartSeqno;
        final long snapshotEndSeqno;

        if (origSnapshotStartSeqno == startSeqno + 1) {
          // startSeqno must be >= snapshotStartSeqno. If we get here, then we probably received
          // a snapshot marker and then disconnected before receiving the first seqno in the snapshot.
          LOGGER.debug("Disregarding snapshot marker from the future.");
          snapshotStartSeqno = startSeqno;
          snapshotEndSeqno = startSeqno;
        } else {
          snapshotEndSeqno = origSnapshotEndSeqno;
          snapshotStartSeqno = origSnapshotStartSeqno;
        }

        LOGGER.debug("Opening Stream against {} with vbid: {}, vbuuid: {}, startSeqno: {}, " +
                "endSeqno: {},  snapshotStartSeqno: {}, snapshotEndSeqno: {}",
            inetAddress, vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);

        ByteBuf buffer = Unpooled.buffer();
        DcpOpenStreamRequest.init(buffer, vbid);
        DcpOpenStreamRequest.vbuuid(buffer, vbuuid);
        DcpOpenStreamRequest.startSeqno(buffer, startSeqno);
        DcpOpenStreamRequest.endSeqno(buffer, endSeqno);
        DcpOpenStreamRequest.snapshotStartSeqno(buffer, snapshotStartSeqno);
        DcpOpenStreamRequest.snapshotEndSeqno(buffer, snapshotEndSeqno);

        sendRequest(buffer).addListener(new DcpResponseListener() {
          @Override
          public void operationComplete(Future<DcpResponse> future) throws Exception {
            if (!future.isSuccess()) {
              LOGGER.debug("Failed open Stream against {} with vbid: {}", inetAddress, vbid);
              streamIsOpen.set(vbid, false);
              subscriber.onError(future.cause());
              return;
            }

            ByteBuf buf = future.getNow().buffer();
            try {
              final ResponseStatus status = MessageUtil.getResponseStatus(buf);

              if (status == KEY_EXISTS) {
                LOGGER.debug("Stream already open against {} with vbid: {}", inetAddress, vbid);
                subscriber.onCompleted();
                return;
              }

              if (!status.isSuccess()) {
                LOGGER.debug("Failed open Stream against {} with vbid: {}", inetAddress, vbid);
                streamIsOpen.set(vbid, false);
              }

              if (status.isSuccess()) {
                LOGGER.debug("Opened Stream against {} with vbid: {}", inetAddress, vbid);
                streamIsOpen.set(vbid, true);

                subscriber.onCompleted();

                ByteBuf flog = Unpooled.buffer();
                DcpFailoverLogResponse.init(flog);
                DcpFailoverLogResponse.vbucket(flog, DcpOpenStreamResponse.vbucket(buf));
                ByteBuf content = MessageUtil.getContent(buf).copy().writeShort(vbid);
                MessageUtil.setContent(content, flog);
                content.release();
                env.controlEventHandler().onEvent(dummyFlowController, flog);

              } else if (status == ROLLBACK_REQUIRED) {
                subscriber.onError(new RollbackException());

                // create a rollback message and emit
                ByteBuf rb = Unpooled.buffer();
                RollbackMessage.init(rb, vbid, DcpOpenStreamResponse.rollbackSeqno(buf));
                env.controlEventHandler().onEvent(dummyFlowController, rb);

              } else if (status == NOT_MY_VBUCKET) {
                subscriber.onError(new NotMyVbucketException());

              } else {
                subscriber.onError(new IllegalStateException("Unhandled Status: " + status));
              }
            } finally {
              buf.release();
            }
          }
        });
      }
    });
  }

  public Completable closeStream(final short vbid) {
    return Completable.create(new Completable.OnSubscribe() {
      @Override
      public void call(final CompletableSubscriber subscriber) {
        if (state() != LifecycleState.CONNECTED) {
          subscriber.onError(new NotConnectedException());
          return;
        }

        LOGGER.debug("Closing Stream against {} with vbid: {}", inetAddress, vbid);

        ByteBuf buffer = Unpooled.buffer();
        DcpCloseStreamRequest.init(buffer);
        DcpCloseStreamRequest.vbucket(buffer, vbid);

        sendRequest(buffer).addListener(new DcpResponseListener() {
          @Override
          public void operationComplete(Future<DcpResponse> future) throws Exception {
            streamIsOpen.set(vbid, false);
            if (future.isSuccess()) {
              future.getNow().buffer().release();
              LOGGER.debug("Closed Stream against {} with vbid: {}", inetAddress, vbid);
              subscriber.onCompleted();
            } else {
              LOGGER.debug("Failed close Stream against {} with vbid: {}", inetAddress, vbid);
              subscriber.onError(future.cause());
            }
          }
        });
      }
    });
  }

  /**
   * Returns all seqnos for all vbuckets on that channel.
   */
  public Single<ByteBuf> getSeqnos() {
    return Single.create(new Single.OnSubscribe<ByteBuf>() {
      @Override
      public void call(final SingleSubscriber<? super ByteBuf> subscriber) {
        if (state() != LifecycleState.CONNECTED) {
          subscriber.onError(new NotConnectedException());
          return;
        }

        ByteBuf buffer = Unpooled.buffer();
        DcpGetPartitionSeqnosRequest.init(buffer);
        DcpGetPartitionSeqnosRequest.vbucketState(buffer, VbucketState.ACTIVE);

        sendRequest(buffer).addListener(new DcpResponseListener() {
          @Override
          public void operationComplete(Future<DcpResponse> future) throws Exception {
            if (future.isSuccess()) {
              ByteBuf buf = future.getNow().buffer();
              try {
                subscriber.onSuccess(MessageUtil.getContent(buf).copy());
              } finally {
                buf.release();
              }
            } else {
              subscriber.onError(future.cause());
            }
          }
        });
      }
    });
  }

  public Single<ByteBuf> getFailoverLog(final short vbid) {
    return Single.create(new Single.OnSubscribe<ByteBuf>() {
      @Override
      public void call(final SingleSubscriber<? super ByteBuf> subscriber) {
        if (state() != LifecycleState.CONNECTED) {
          subscriber.onError(new NotConnectedException());
          return;
        }

        ByteBuf buffer = Unpooled.buffer();
        DcpFailoverLogRequest.init(buffer);
        DcpFailoverLogRequest.vbucket(buffer, vbid);

        LOGGER.debug("Asked for failover log on {} for vbid: {}", inetAddress, vbid);
        sendRequest(buffer).addListener(new DcpResponseListener() {
          @Override
          public void operationComplete(Future<DcpResponse> future) throws Exception {
            if (!future.isSuccess()) {
              LOGGER.debug("Failed to ask for failover log on {} for vbid: {}", inetAddress, vbid);
              subscriber.onError(future.cause());
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
              subscriber.onSuccess(flog);

            } finally {
              buf.release();
            }
          }
        });
      }
    });
  }

  public boolean streamIsOpen(short vbid) {
    return streamIsOpen.get(vbid);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof InetAddress) {
      return inetAddress.equals(o);
    } else if (o instanceof DcpChannel) {
      return inetAddress.equals(((DcpChannel) o).inetAddress);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return inetAddress.hashCode();
  }

  @Override
  public String toString() {
    return "DcpChannel{inetAddress=" + inetAddress + '}';
  }
}
