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

package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.ChannelOutboundHandlerAdapter;
import com.couchbase.client.deps.io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Collaborates with {@link ConnectInterceptingHandler} to fail the connection
 * if the handshake takes too long.
 */
public class HandshakeTimeoutHandler extends ChannelOutboundHandlerAdapter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HandshakeTimeoutHandler.class);

  private static final HandshakeDeadlineEvent HANDSHAKE_DEADLINE_EVENT = new HandshakeDeadlineEvent();

  private final long timeout;
  private final TimeUnit timeoutUnit;

  public HandshakeTimeoutHandler(long timeout, TimeUnit timeoutUnit) {
    this.timeout = timeout;
    this.timeoutUnit = requireNonNull(timeoutUnit);
  }

  @Override
  public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                      final SocketAddress localAddress, final ChannelPromise promise) throws Exception {
    ctx.connect(remoteAddress, localAddress, promise);
    ctx.pipeline().remove(this);

    final Runnable fireDeadlineEvent = new Runnable() {
      /**
       * If there is a {@link ConnectInterceptingHandler} remaining in the pipeline,
       * it will respond to this event by failing the connection. If there are none
       * in the pipeline, the handshake is complete and this event is ignored.
       */
      @Override
      public void run() {
        LOGGER.debug("Firing handshake deadline event.");
        ctx.fireUserEventTriggered(HANDSHAKE_DEADLINE_EVENT);
      }
    };

    LOGGER.debug("Handshake timeout is {} {}.", timeout, timeoutUnit);
    ctx.executor().schedule(fireDeadlineEvent, timeout, timeoutUnit);
  }
}
