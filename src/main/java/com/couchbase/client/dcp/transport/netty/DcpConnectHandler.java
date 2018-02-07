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
import com.couchbase.client.dcp.message.BucketSelectRequest;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import com.couchbase.client.dcp.message.HelloRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.VersionRequest;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

/**
 * Opens the DCP connection on the channel and once established removes itself.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpConnectHandler extends ConnectInterceptingHandler<ByteBuf> {

    /**
     * MEMCACHED Status indicating a success
     */
    private static final byte SUCCESS = 0x00;

    /**
     * Connection steps
     */
    private static final byte VERSION = 0;
    private static final byte HELLO = 1;
    private static final byte SELECT = 2;
    private static final byte OPEN = 3;
    private static final byte REMOVE = 4;

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
     * The bucket name used with select bucket request
     */
    private final String bucket;

    /**
     * The current connection step
     */
    private byte step = VERSION;

    /**
     * Creates a new connect handler.
     *
     * @param connectionNameGenerator
     *            the generator of the connection names.
     */
    DcpConnectHandler(final ConnectionNameGenerator connectionNameGenerator, final String bucket) {
        this.connectionNameGenerator = connectionNameGenerator;
        this.bucket = bucket;
    }

    /**
     * Once the channel becomes active, sends the initial open connection request.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        ByteBuf request = ctx.alloc().buffer();
        VersionRequest.init(request);
        ctx.writeAndFlush(request);
    }

    /**
     * Once we get a response from the connect request, check if it is successful and complete/fail the connect
     * phase accordingly.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        short status = MessageUtil.getStatus(msg);
        if (status == SUCCESS) {
            step++;
            switch (step) {
                case HELLO:
                    hello(ctx, msg);
                    break;
                case SELECT:
                    select(ctx);
                    break;
                case OPEN:
                    open(ctx);
                    break;
                case REMOVE:
                    remove(ctx);
                    break;
                default:
                    originalPromise().setFailure(new IllegalStateException("Unidentified DcpConnection step " + step));
                    break;
            }
        } else {
            originalPromise().setFailure(new IllegalStateException("Could not open DCP Connection: Failed in the "
                    + toString(step) + " step, response status is " + status));
        }
    }

    private String toString(byte step) {
        switch (step) {
            case VERSION:
                return "VERSION";
            case HELLO:
                return "HELLO";
            case SELECT:
                return "SELECT";
            case OPEN:
                return "OPEN";
            case REMOVE:
                return "REMOVE";
            default:
                return "UNKNOWN";
        }
    }

    private void select(ChannelHandlerContext ctx) {
        // Select bucket
        ByteBuf request = ctx.alloc().buffer();
        BucketSelectRequest.init(request, bucket);
        ctx.writeAndFlush(request);
    }

    private void remove(ChannelHandlerContext ctx) {
        ctx.pipeline().remove(this);
        originalPromise().setSuccess();
        ctx.fireChannelActive();
        LOGGER.debug("DCP Connection opened with Name \"{}\" against Node {}", connectionName,
                ctx.channel().remoteAddress());
    }

    private void open(ChannelHandlerContext ctx) {
        ByteBuf request = ctx.alloc().buffer();
        DcpOpenConnectionRequest.init(request);
        DcpOpenConnectionRequest.connectionName(request, Unpooled.copiedBuffer(connectionName, CharsetUtil.UTF_8));
        ctx.writeAndFlush(request);

    }

    private void hello(ChannelHandlerContext ctx, ByteBuf msg) {
        connectionName = connectionNameGenerator.name();
        String response = MessageUtil.getContentAsString(msg);
        int majorVersion;
        try {
            majorVersion = Integer.parseInt(response.substring(0, 1));
        } catch (NumberFormatException e) {
            originalPromise().setFailure(
                    new IllegalStateException("Version returned by the server couldn't be parsed " + response, e));
            ctx.close(ctx.voidPromise());
            return;
        }
        if (majorVersion < 5) {
            step = OPEN;
            open(ctx);
        } else {
            ByteBuf request = ctx.alloc().buffer();
            HelloRequest.init(request, Unpooled.copiedBuffer(connectionName, CharsetUtil.UTF_8));
            ctx.writeAndFlush(request);
        }
    }

}
