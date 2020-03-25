/*
 * Copyright 2020 Couchbase, Inc.
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

import com.couchbase.client.dcp.conductor.BucketConfigSink;
import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.message.ServerRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.message.HelloFeature.CLUSTERMAP_CHANGE_NOTIFICATION;
import static com.couchbase.client.dcp.message.MessageUtil.GET_CLUSTER_CONFIG_OPCODE;
import static java.util.Objects.requireNonNull;

public class BucketConfigHandler extends SimpleChannelInboundHandler<ByteBuf> {
  private static final Logger log = LoggerFactory.getLogger(BucketConfigHandler.class);

  private final BucketConfigSink bucketConfigSink;
  private final Duration configRefreshInterval;

  BucketConfigHandler(BucketConfigSink bucketConfigSink, Duration configRefreshInterval) {
    this.bucketConfigSink = requireNonNull(bucketConfigSink);
    this.configRefreshInterval = requireNonNull(configRefreshInterval);
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    final byte magic = msg.getByte(0);

    // handle server-push config change notifications
    if (magic == MessageUtil.MAGIC_SERVER_REQ) {
      ServerRequest.handleServerRequest(ctx, msg, bucketConfigSink);
      return;
    }

    // pass along everything that isn't a response
    if (magic != MessageUtil.MAGIC_RES) {
      ctx.fireChannelRead(msg.retain());
      return;
    }

    // handle a client-initiated cluster config response
    final byte opcode = msg.getByte(1);
    if (opcode == MessageUtil.GET_CLUSTER_CONFIG_OPCODE) {
      updateConfig(ctx, msg, "client request");

      final boolean serverPushesConfigUpdates = CLUSTERMAP_CHANGE_NOTIFICATION.isEnabled(ctx);
      if (!serverPushesConfigUpdates) {
        scheduleConfigRefresh(ctx);
      }

      return;
    }

    // Couchbase Server has a config option called `dedupe_nmvb_maps` (defaults to false in 6.5.0).
    // When this option is enabled, the server will send each config rev to the client at most once,
    // regardless of whether the config was sent in a clustermap change notification or a
    // NotMyVbucket (NMVB) response. If the NMVB is sent first, the client will not receive
    // a clustermap change notification. This means the client needs to look in NMVB responses
    // to see if they contain configs, or risk missing config updates.
    //
    // Trond Norbye explains:
    // """
    // When memcached receives a new cluster map from ns_server it schedules a thread to iterate over
    // all connections to schedule a push of the new cluster map to all clients who enabled this feature
    // by adding a "server event" to its internal list. The next time the connection gets scheduled it'll
    // start off by completing the ongoing command (unless OoO is enabled) and before starting the next
    // command it'll iterate over the list of server events for the connection. The server event for
    // pushing the cluster map starts off by comparing the sequence number for the buckets cluster map
    // with the sequence number we've already sent to the client, and it'll only send the cluster map
    // to the client if the revision number of the buckets cluster map is newer than what we've sent
    // to the client (either through a push notification or through not my vbucket).
    // """
    if (isNotMyVbucketError(msg)) {
      updateConfig(ctx, msg, "NotMyVbucket");
      // don't return yet; need to pass the error response down the handler chain.
    }

    ctx.fireChannelRead(msg.retain());
  }

  private void scheduleConfigRefresh(ChannelHandlerContext ctx) {
    log.debug("Scheduling bucket config refresh in {}", configRefreshInterval);

    ctx.executor().schedule(() -> {
      ByteBuf request = ctx.alloc().buffer();
      MessageUtil.initRequest(GET_CLUSTER_CONFIG_OPCODE, request);
      ctx.writeAndFlush(request).addListener(future -> {
        if (!future.isSuccess() && ctx.channel().isActive()) {
          log.warn("Config refresh failed; rescheduling.", future.cause());
          scheduleConfigRefresh(ctx);
        }
      });
    }, configRefreshInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  private static boolean isNotMyVbucketError(ByteBuf msg) {
    return MessageUtil.getResponseStatus(msg) == ResponseStatus.NOT_MY_VBUCKET;
  }

  private void updateConfig(ChannelHandlerContext ctx, ByteBuf msg, String source) {
    String clustermap = MessageUtil.getContentAsString(msg);
    if (!clustermap.isEmpty()) {
      log.info("{} Received bucket config from {}", system(ctx.channel()), source);

      InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
      bucketConfigSink.accept(new HostAndPort(remote), clustermap);
    }
  }
}
