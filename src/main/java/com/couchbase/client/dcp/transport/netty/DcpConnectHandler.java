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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.ChannelOutboundHandler;
import com.couchbase.client.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;

import java.net.SocketAddress;

/**
 * Opens the DCP connection on the channel and once established removes itself.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpConnectHandler
    extends SimpleChannelInboundHandler<ByteBuf>
    implements ChannelOutboundHandler {

    /**
     * Status indicating a successful open connect attempt.
     */
    private static final byte CONNECT_SUCCESS = 0x00;

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpConnectHandler.class);

    /**
     * Generates the connection name for the dcp connection.
     */
    private final ConnectionNameGenerator connectionNameGenerator;

    /**
     * The generated connection name, set fresh once a channel becomes active.
     */
    private String connectionName;

    /**
     * The original connect promise stored for later fulfillment.
     */
    private ChannelPromise originalPromise;

    /**
     * Creates a new connect handler.
     *
     * @param connectionNameGenerator the generator of the connection names.
     */
    DcpConnectHandler(final ConnectionNameGenerator connectionNameGenerator) {
        this.connectionNameGenerator = connectionNameGenerator;
    }

    /**
     * Once the channel becomes active, sends the initial open connection request.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        connectionName = connectionNameGenerator.name();
        ByteBuf request = ctx.alloc().buffer();
        DcpOpenConnectionRequest.init(request);
        DcpOpenConnectionRequest.connectionName(request, Unpooled.copiedBuffer(connectionName, CharsetUtil.UTF_8));
        ctx.writeAndFlush(request);
    }

    /**
     * Once we get a response from the connect request, check if it is successful and complete/fail the connect
     * phase accordingly.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        short status = MessageUtil.getStatus(msg);
        if (status == CONNECT_SUCCESS) {
            ctx.pipeline().remove(this);
            originalPromise.setSuccess();
            ctx.fireChannelActive();
            LOGGER.debug("DCP Connection opened with Name \"{}\" against Node {}", connectionName,
                ctx.channel().remoteAddress());
        } else {
            originalPromise.setFailure(new IllegalStateException("Could not open DCP Connection: Status is " + status));
        }
    }

    /**
     * Intercept the connect phase and store the original promise.
     */
    @Override
    public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
        final SocketAddress localAddress, ChannelPromise promise) throws Exception {
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
        ctx.read();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
}
