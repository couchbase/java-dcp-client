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
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.BucketSelectRequest;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import com.couchbase.client.dcp.message.HelloRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.message.VersionRequest;
import com.couchbase.client.dcp.util.Version;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.util.AttributeKey;

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
   * The version reported by the server.
   */
  private static final AttributeKey<Version> SERVER_VERSION = AttributeKey.valueOf("serverVersion");

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
   * Tells us what features to advertise with the HELLO request.
   */
  private final DcpControl dcpControl;

  /**
   * The current connection step
   */
  private byte step = VERSION;

  /**
   * Returns the Couchbase Server version associated with the given channel.
   *
   * @throws IllegalStateException if {@link DcpConnectHandler} has not yet issued
   *                               a Version request and processed the result.
   */
  public static Version getServerVersion(Channel channel) {
    Version version = channel.attr(SERVER_VERSION).get();
    if (version == null) {
      throw new IllegalStateException("Server version attribute not yet set by "
          + DcpConnectHandler.class.getSimpleName());
    }
    return version;
  }

  /**
   * Creates a new connect handler.
   *
   * @param connectionNameGenerator the generator of the connection names.
   */
  DcpConnectHandler(final ConnectionNameGenerator connectionNameGenerator, final String bucket, final DcpControl dcpControl) {
    this.connectionNameGenerator = connectionNameGenerator;
    this.bucket = bucket;
    this.dcpControl = dcpControl;
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
    ResponseStatus status = MessageUtil.getResponseStatus(msg);
    if (status.isSuccess()) {
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
    DcpOpenConnectionRequest.connectionName(request, connectionName);
    ctx.writeAndFlush(request);

  }

  private void hello(ChannelHandlerContext ctx, ByteBuf msg) {
    connectionName = connectionNameGenerator.name();
    final String versionString = MessageUtil.getContentAsString(msg);
    final Version serverVersion;
    try {
      serverVersion = Version.parseVersion(versionString);
      ctx.channel().attr(SERVER_VERSION).set(serverVersion);

    } catch (IllegalArgumentException e) {
      originalPromise().setFailure(
          new IllegalStateException("Version returned by the server couldn't be parsed " + versionString, e));
      ctx.close(ctx.voidPromise());
      return;
    }

    if (serverVersion.major() < 5) {
      // Skip HELLO and SELECT
      step = OPEN;
      open(ctx);
      return;
    }

    final CompressionMode compressionMode = dcpControl.compression(serverVersion);
    short[] extraFeatures = compressionMode.getHelloFeatures(serverVersion);
    ByteBuf request = ctx.alloc().buffer();
    HelloRequest.init(request, connectionName, extraFeatures);
    ctx.writeAndFlush(request);
  }

}
