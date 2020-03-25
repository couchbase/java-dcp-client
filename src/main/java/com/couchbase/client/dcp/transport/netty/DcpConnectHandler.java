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

import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.buffer.DcpOps;
import com.couchbase.client.dcp.config.CompressionMode;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.BucketSelectRequest;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import com.couchbase.client.dcp.message.HelloFeature;
import com.couchbase.client.dcp.message.HelloRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.message.VersionRequest;
import com.couchbase.client.dcp.util.Version;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.message.HelloFeature.SELECT_BUCKET;
import static com.couchbase.client.dcp.message.MessageUtil.GET_CLUSTER_CONFIG_OPCODE;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

/**
 * Opens the DCP connection on the channel and once established removes itself.
 */
public class DcpConnectHandler extends ConnectInterceptingHandler<ByteBuf> {

  private abstract class ConnectionStep {
    private final String name;

    ConnectionStep(String name) {
      this.name = requireNonNull(name);
    }

    @Override
    public String toString() {
      return name;
    }

    abstract void issueRequest(ChannelHandlerContext ctx);

    /**
     * @return the next connection step to execute
     */
    abstract ConnectionStep handleResponse(ChannelHandlerContext ctx, ByteBuf msg);
  }

  private final ConnectionStep version = new ConnectionStep("version") {
    @Override
    void issueRequest(ChannelHandlerContext ctx) {
      ByteBuf request = ctx.alloc().buffer();
      VersionRequest.init(request);
      ctx.writeAndFlush(request);
    }

    @Override
    ConnectionStep handleResponse(ChannelHandlerContext ctx, ByteBuf msg) {
      final String versionString = MessageUtil.getContentAsString(msg);
      LOGGER.info("{} Couchbase Server version {}", system(ctx.channel()), versionString);
      final Version serverVersion = Version.parseVersion(versionString);
      ctx.channel().attr(SERVER_VERSION).set(serverVersion);

      return hello;
    }
  };

  private final ConnectionStep hello = new ConnectionStep("hello") {
    @Override
    void issueRequest(ChannelHandlerContext ctx) {
      final Version serverVersion = getServerVersion(ctx.channel());
      final CompressionMode compressionMode = dcpControl.compression(serverVersion);
      final Set<HelloFeature> extraFeatures = compressionMode.getHelloFeatures(serverVersion);

      ByteBuf request = ctx.alloc().buffer();
      HelloRequest.init(request, connectionName, extraFeatures);
      ctx.writeAndFlush(request);
    }

    @Override
    ConnectionStep handleResponse(ChannelHandlerContext ctx, ByteBuf msg) {
      final Set<HelloFeature> features = HelloRequest.parseResponse(msg);
      LOGGER.info("{} Negotiated features: {}", ctx.channel(), features);
      ctx.channel().attr(NEGOTIATED_FEATURES).set(unmodifiableSet(features));

      // skip the 'select bucket' step if unsupported
      return features.contains(SELECT_BUCKET) ? selectBucket : open;
    }
  };

  private final ConnectionStep selectBucket = new ConnectionStep("select bucket") {
    @Override
    void issueRequest(ChannelHandlerContext ctx) {
      ByteBuf request = ctx.alloc().buffer();
      BucketSelectRequest.init(request, bucket);
      ctx.writeAndFlush(request);
    }

    @Override
    ConnectionStep handleResponse(ChannelHandlerContext ctx, ByteBuf msg) {
      return open;
    }
  };

  private final ConnectionStep open = new ConnectionStep("open") {
    @Override
    void issueRequest(ChannelHandlerContext ctx) {
      ByteBuf request = ctx.alloc().buffer();
      DcpOpenConnectionRequest.init(request);
      DcpOpenConnectionRequest.connectionName(request, connectionName);
      ctx.writeAndFlush(request);
    }

    @Override
    ConnectionStep handleResponse(ChannelHandlerContext ctx, ByteBuf msg) {
      return remove;
    }
  };

  private final ConnectionStep remove = new ConnectionStep("remove") {
    @Override
    void issueRequest(ChannelHandlerContext ctx) {
      // Get the bucket config. BucketConfigHandler will handle the response.
      ByteBuf request = ctx.alloc().buffer();
      MessageUtil.initRequest(GET_CLUSTER_CONFIG_OPCODE, request);
      ctx.writeAndFlush(request);

      ctx.pipeline().remove(DcpConnectHandler.this);
      originalPromise().setSuccess();
      ctx.fireChannelActive();
      LOGGER.debug("DCP Connection opened with Name \"{}\" against Node {}", connectionName,
          ctx.channel().remoteAddress());
    }

    @Override
    ConnectionStep handleResponse(ChannelHandlerContext ctx, ByteBuf msg) {
      throw new AssertionError("Connection step '" + this + "' should not have a response to handle.");
    }
  };

  /**
   * The logger used.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(DcpConnectHandler.class);

  /**
   * The version reported by the server.
   */
  private static final AttributeKey<Version> SERVER_VERSION = AttributeKey.valueOf("serverVersion");

  /**
   * The features returned by the server in the HELO response.
   */
  private static final AttributeKey<Set<HelloFeature>> NEGOTIATED_FEATURES = AttributeKey.valueOf("negotiatedFeatures");

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
  private ConnectionStep step = version;

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
   * Returns the features from the HELO response (the intersection of the features
   * we advertised and the features supported by the server).
   *
   * @throws IllegalStateException if {@link DcpConnectHandler} has not yet issued
   *                               a HELO request and processed the result.
   */
  public static Set<HelloFeature> getFeatures(Channel channel) {
    Set<HelloFeature> features = channel.attr(NEGOTIATED_FEATURES).get();
    if (features == null) {
      throw new IllegalStateException("Negotiated features attribute not yet set by "
          + DcpConnectHandler.class.getSimpleName());
    }
    return features;
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
   * Assigns a name to the connection and starts the first connection step.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    try {
      connectionName = connectionNameGenerator.name();
      step.issueRequest(ctx);

    } catch (Throwable t) {
      fail(ctx, t);
    }
  }

  /**
   * Once we get a response from the connect request, check if it is successful and complete/fail the connect
   * phase accordingly.
   */
  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
    try {
      ResponseStatus status = MessageUtil.getResponseStatus(msg);
      if (!status.isSuccess()) {
        throw new DcpOps.BadResponseStatusException(status);
      }

      step = step.handleResponse(ctx, msg);
      step.issueRequest(ctx);

    } catch (Throwable t) {
      fail(ctx, new RuntimeException("Could not establish DCP connection; failed in the '" + step + "' step; " + t, t));
    }
  }

  private void fail(final ChannelHandlerContext ctx, Throwable t) {
    originalPromise().setFailure(t);
    ctx.channel().close();
  }
}
