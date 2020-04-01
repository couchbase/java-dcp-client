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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.ConnectException;
import java.net.SocketAddress;

/**
 * Helper class which intercepts the connect promise and stores it for later reuse.
 * <p>
 * Subclasses are expected that once their stuff is complete take the {@link #originalPromise()}
 * and complete or fail it accordingly, otherwise the connect chain will be stuck forever and
 * maybe time out depending on the channel config.
 */
abstract class ConnectInterceptingHandler<T>
    extends SimpleChannelInboundHandler<T>
    implements ChannelOutboundHandler {

  /**
   * The original connect promise which is intercepted and then completed/failed after the
   * authentication procedure.
   */
  private ChannelPromise originalPromise;

  /**
   * Returns the intercepted original connect promise for completion or failure.
   */
  ChannelPromise originalPromise() {
    return originalPromise;
  }

  /**
   * Intercept the connect phase and store the original promise.
   */
  @Override
  public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
                      final SocketAddress localAddress, final ChannelPromise promise) throws Exception {
    originalPromise = promise;
    ChannelPromise inboundPromise = ctx.newPromise();
    inboundPromise.addListener(new GenericFutureListener<Future<Void>>() {
      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        if (!future.isSuccess() && !originalPromise.isDone()) {
          originalPromise.setFailure(future.cause());
        }
      }
    });
    ctx.connect(remoteAddress, localAddress, inboundPromise);
  }

  @Override
  public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
    ctx.bind(localAddress, promise);
  }

  @Override
  public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    ctx.disconnect(promise);
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    ctx.close(promise);
  }

  @Override
  public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    ctx.deregister(promise);
  }

  @Override
  public void read(ChannelHandlerContext ctx) throws Exception {
    ctx.read();
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    ctx.write(msg, promise);
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (originalPromise != null) {
      originalPromise.setFailure(cause);
    }
    ctx.fireExceptionCaught(cause);
  }

  private static final String customPortAdvice = "If your seed nodes include a custom port," +
      " make sure it's the port of the KV service which defaults to 11210 (or 11207 for TLS).";

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof HandshakeDeadlineEvent) {
      originalPromise().tryFailure(new ConnectTimeoutException("Handshake did not complete before deadline. " + customPortAdvice));
      ctx.close();
      return;
    }

    ctx.fireUserEventTriggered(evt);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    originalPromise().tryFailure(new ConnectException("Channel became inactive before handshake completed. " + customPortAdvice));
    ctx.fireChannelInactive();
  }
}
