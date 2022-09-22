/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.buffer.DcpRequestDispatcher;
import com.couchbase.client.dcp.conductor.DcpChannelControlHandler;
import com.couchbase.client.dcp.core.state.NotConnectedException;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpNoopRequest;
import com.couchbase.client.dcp.message.DcpNoopResponse;
import com.couchbase.client.dcp.message.DcpSeqnoAdvancedRequest;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.DcpSystemEventRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.metrics.DcpChannelMetrics;
import com.couchbase.client.dcp.events.Tracer;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelInboundHandlerAdapter;
import com.couchbase.client.core.deps.io.netty.handler.timeout.IdleState;
import com.couchbase.client.core.deps.io.netty.handler.timeout.IdleStateEvent;
import com.couchbase.client.core.deps.io.netty.util.concurrent.EventExecutor;
import com.couchbase.client.core.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.core.deps.io.netty.util.concurrent.ImmediateEventExecutor;
import com.couchbase.client.core.deps.io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil.safeRelease;
import static java.util.Objects.requireNonNull;

/**
 * Handles the "business logic" of incoming DCP mutation and control messages.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpMessageHandler extends ChannelInboundHandlerAdapter implements DcpRequestDispatcher {

  // IMPLEMENTATION NOTES
  //
  // This code takes advantage of some nice properties of Netty's event loop:
  //
  // * Only one thread at a time can be running in a channel's event loop.
  //
  // * Code running in a channel's event loop has visibility of all previous
  //   memory writes performed in the same event loop.
  //
  // Another way to think about it: any code running in the event loop is effectively
  // synchronized with respect to all other code running in the event loop.
  //
  // In Netty 4.x this is because each channel has a single dedicated event loop thread.
  // In Netty 5 the threading model will change, but the guarantees remain the same.
  //
  // Reference:
  //   http://netty.io/wiki/new-and-noteworthy-in-5.0.html#even-more-flexible-thread-model

  /**
   * The logger used.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(DcpMessageHandler.class);

  /**
   * The data callback where the events are fed to the user.
   */
  private final DataEventHandler dataEventHandler;

  /**
   * The handler for the control events since they need more advanced handling up the stack.
   */
  private final DcpChannelControlHandler controlHandler;

  private final ChannelFlowController flowController;

  private final DcpChannelMetrics metrics;

  private final Tracer tracer;

  /**
   * A counter for assigning an ID to each request. There should never be
   * two outstanding requests with the same ID on the same channel.
   * <p>
   * Must only be accessed/modified by the event loop thread.
   */
  private int nextOpaque = Integer.MIN_VALUE;

  private static class OutstandingRequest {
    private final int opaque;
    private final Promise<DcpResponse> promise;

    private OutstandingRequest(int opaque, Promise<DcpResponse> promise) {
      this.opaque = opaque;
      this.promise = requireNonNull(promise);
    }
  }

  /**
   * A reference to the channel context so {@link #sendRequest} can write
   * messages to the channel.
   * <p>
   * A non-null value indicates the channel is active and accepting requests.
   * The value is initialized when the channel becomes active, and set back
   * to null when the channel becomes inactive.
   * <p>
   * It's volatile so the value set by {@link #channelActive} in the event loop
   * thread is visible when {@link #sendRequest} is called from another thread.
   */
  private volatile ChannelHandlerContext volatileContext;

  /**
   * Holds the promises issued by {@link #sendRequest} that have not yet
   * been fulfilled by {@link #channelRead} or failed by {@link #channelInactive}.
   * <p>
   * Must only be accessed/modified by the event loop thread.
   */
  private final Queue<OutstandingRequest> outstandingRequests = new ArrayDeque<>();

  /**
   * Create a new message handler.
   *
   * @param environment data event callback handler.
   * @param controlHandler control event handler.
   */
  DcpMessageHandler(final Channel channel, final Client.Environment environment,
                    final DcpChannelControlHandler controlHandler,
                    final DcpChannelMetrics metrics) {
    this.dataEventHandler = environment.dataEventHandler();
    this.controlHandler = controlHandler;
    this.flowController = new ChannelFlowControllerImpl(channel, environment);
    this.metrics = requireNonNull(metrics);
    this.tracer = environment.tracer();
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    tracer.onConnectionOpen(String.valueOf(ctx.channel()));

    this.volatileContext = ctx;
    super.channelActive(ctx);
  }

  /**
   * Fails all the promises in the {@link #outstandingRequests} queue when the
   * channel becomes inactive.
   * <p>
   * Netty always invokes this method in the event loop thread. To ensure
   * this method never runs concurrently with {@link #unsafeSendRequest},
   * we only call that method in the event loop thread as well.
   */
  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    tracer.onConnectionClose(String.valueOf(ctx.channel()));

    volatileContext = null; // Make sure future `sendRequest` calls fail.

    Exception connectionClosed = new NotConnectedException("Channel became inactive while awaiting response.");
    for (OutstandingRequest request : outstandingRequests) {
      try {
        request.promise.setFailure(connectionClosed);
      } catch (Throwable t) {
        LOGGER.error("Failed to set promise failure", t);
      }
    }
    outstandingRequests.clear();

    super.channelInactive(ctx);
  }

  public Future<DcpResponse> sendRequest(final ByteBuf request) {
    // Since this method might be called from outside the event loop thread,
    // it's possible for `channelInactive` to run concurrently and set
    // `volatileContext` to null at any time. Take a snapshot so the value
    // doesn't change out from under us!
    final ChannelHandlerContext ctx = volatileContext;

    // Regardless of whether we're in the event loop thread, a null context
    // indicates the channel is inactive and not receiving requests.
    if (ctx == null) {
      safeRelease(request);
      return ImmediateEventExecutor.INSTANCE.newFailedFuture(
          new NotConnectedException("Failed to issue request; channel is not active."));
    }

    final EventExecutor executor = ctx.executor();
    final Promise<DcpResponse> promise = executor.newPromise();

    // Here's where the paths diverge. If we're in the event loop thread,
    // it's impossible for channelInactive to be running at the same time,
    // so we can safely enqueue the request immediately.
    if (executor.inEventLoop()) {
      unsafeSendRequest(ctx, request, promise);
      return promise;
    }

    // If we got here, we're not running in the event loop thread.
    // To prevent `channelInactive` from interfering while the request
    // is enqueued, schedule a task to send the request later in the
    // event loop thread.
    //
    // Netty would reschedule the `writeAndFlush` operation to run in the
    // event loop thread anyway, so there shouldn't be much additional
    // overhead in doing the scheduling ourselves.
    //
    // Another reason to reschedule: Netty does not guarantee ordering
    // between writes initiated in the event loop and writes initiated
    // outside the event loop. In order to guarantee the outstanding
    // requests get enqueued in the same order they are written to the
    // channel, it's necessary for all write + enqueue operations to happen
    // in the event loop. See https://github.com/netty/netty/issues/3887
    try {
      executor.submit(
          new Runnable() {
            @Override
            public void run() {
              // The channel may have become inactive after this task
              // was submitted, so check one more time.
              //
              // Since we're now in the event loop thread, there's no risk of
              // channelInactive() changing the value of `volatileContext`.
              // This assignment is just to avoid redundant volatile reads.
              final ChannelHandlerContext ctx = volatileContext;
              if (ctx == null) {
                safeRelease(request);
                promise.setFailure(new NotConnectedException("Failed to issue request; channel is not active."));
              } else {
                unsafeSendRequest(ctx, request, promise);
              }
            }
          }
      );

    } catch (Throwable t) {
      safeRelease(request);
      promise.setFailure(t);
    }

    return promise;
  }

  /**
   * Writes the request to the channel and records the promise in the
   * outstanding request queue.
   * <p>
   * "Unsafe" because this method must only be called in the event loop thread,
   * to ensure it never runs concurrently with {@link #channelInactive},
   * and so that outstanding requests are enqueued in the same order
   * they are written to the channel.
   */
  private void unsafeSendRequest(final ChannelHandlerContext ctx,
                                 final ByteBuf request,
                                 final Promise<DcpResponse> promise) {
    if (!ctx.executor().inEventLoop()) {
      throw new IllegalStateException("Must not be called outside event loop");
    }

    try {
      metrics.trackDcpRequest(promise, request);

      final int opaque = nextOpaque++;
      MessageUtil.setOpaque(opaque, request);

      // Don't need to be notified if/when the bytes are written,
      // so use void promise to save an allocation.
      ctx.writeAndFlush(request, ctx.voidPromise());

      outstandingRequests.add(new OutstandingRequest(opaque, promise));

    } catch (Throwable t) {
      promise.setFailure(t);
    }
  }

  /**
   * Reads server responses and uses them to fulfill promises returned by
   * {@link #sendRequest}.
   * <p>
   * Dispatches other incoming messages to either the data or the control feeds.
   */
  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    final ByteBuf message = (ByteBuf) msg;

    metrics.incrementBytesRead(message.readableBytes());

    // The majority of messages are likely to be stream requests, not responses.
    final byte magic = message.getByte(0);
    if (magic != MessageUtil.MAGIC_RES) {
      handleRequest(ctx, message);
      return;
    }

    // "The current protocol dictates that the server won't start
    // processing the next command until the current command is completely
    // processed (due to the lack of barriers or any other primitives to
    // enforce execution order). The protocol defines some "quiet commands"
    // which won't send responses in certain cases (success for mutations,
    // not found for gets etc). The client would know that such commands
    // was executed when it encounters the response for the next command
    // requested issued by the client."
    // -- https://github.com/couchbase/memcached/blob/master/docs/BinaryProtocol.md#introduction-1

    // The DCP client does not send any "quiet commands", so we assume
    // a 1:1 relationship between requests and responses, and FIFO order.
    final OutstandingRequest request = outstandingRequests.poll();
    if (request == null || MessageUtil.getOpaque(message) != request.opaque) {
      try {
        if (request != null) {
          request.promise.setFailure(new IOException("Response arrived out of order"));
        }

        // Should never happen so long as all requests are made via sendRequest()
        // and successfully written to the channel.
        LOGGER.error("Unexpected response with opaque {} (expected {}); closing connection",
            MessageUtil.getOpaque(message), request == null ? "none" : request.opaque);
        ctx.close();
        return;

      } finally {
        message.release();
      }
    }

    request.promise.setSuccess(new DcpResponse(message));
  }

  /**
   * Close dead connection in response to idle event from IdleStateHandler.
   */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      if (e.state() == IdleState.READER_IDLE) {
        metrics.incrementDeadConnections();
        LOGGER.warn("Closing dead connection.");
        ctx.close();
        return;
      }
    }

    super.userEventTriggered(ctx, evt);
  }

  /**
   * Dispatch incoming message to either the data or the control feeds.
   */
  private void handleRequest(final ChannelHandlerContext ctx, final ByteBuf message) {
    metrics.recordServerRequest(message);

    if (isDataMessage(message)) {
      tracer.onDataEvent(message, ctx.channel());
      dataEventHandler.onEvent(flowController, message);
    } else if (isControlMessage(message)) {
      tracer.onControlEvent(message, ctx.channel());
      controlHandler.onEvent(flowController, message);
    } else if (DcpNoopRequest.is(message)) {
      try {
        ByteBuf buffer = ctx.alloc().buffer();
        DcpNoopResponse.init(buffer);
        MessageUtil.setOpaque(MessageUtil.getOpaque(message), buffer);
        ctx.writeAndFlush(buffer);
      } finally {
        message.release();
      }
    } else {
      try {
        LOGGER.warn("Unknown DCP Message, ignoring. \n{}", MessageUtil.humanize(message));
      } finally {
        message.release();
      }
    }
  }

  /**
   * Helper method to check if the given byte buffer is a control message.
   *
   * @param msg the message to check.
   * @return true if it is, false otherwise.
   */
  private static boolean isControlMessage(final ByteBuf msg) {
    return DcpStreamEndMessage.is(msg)
        || DcpSnapshotMarkerRequest.is(msg)
        || DcpSeqnoAdvancedRequest.is(msg)
        || DcpSystemEventRequest.is(msg);
  }

  /**
   * Helper method to check if the given byte buffer is a data message.
   *
   * @param msg the message to check.
   * @return true if it is, false otherwise.
   */
  private static boolean isDataMessage(final ByteBuf msg) {
    return DcpMutationMessage.is(msg) || DcpDeletionMessage.is(msg) || DcpExpirationMessage.is(msg);
  }
}
