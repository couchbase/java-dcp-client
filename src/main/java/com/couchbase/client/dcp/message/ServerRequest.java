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

package com.couchbase.client.dcp.message;

import com.couchbase.client.dcp.conductor.BucketConfigSink;
import com.couchbase.client.dcp.config.HostAndPort;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;

/**
 * Utils for handling server-initiated requests when DUPLEX mode is active
 */
public class ServerRequest {
  private static final Logger log = LoggerFactory.getLogger(ServerRequest.class);

  private ServerRequest() {
    throw new AssertionError("not instantiable");
  }

  public static boolean isServerRequest(ByteBuf msg) {
    return msg.getByte(0) == MessageUtil.MAGIC_SERVER_REQ;
  }

  public static void handleServerRequest(ChannelHandlerContext ctx, ByteBuf msg, BucketConfigSink bucketConfigSink) {
    if (!isServerRequest(msg)) {
      throw new IllegalArgumentException("expected a server request but got message with magic " + msg.getByte(0));
    }

    switch (msg.getByte(1)) {
      case MessageUtil.CLUSTERMAP_CHANGE_NOTIFICATION_OPCODE:
        handleConfigChangeNotification(ctx, msg, bucketConfigSink);
        break;

      case MessageUtil.AUTHENTICATE_OPCODE:
      case MessageUtil.ACTIVE_EXTERNAL_USERS_OPCODE:
        // Couchbase Server doesn't expect a response to these
        log.warn("Ignoring unexpected server request: {}", MessageUtil.getShortOpcodeName(msg));
        break;

      default:
        log.warn("Ignoring unrecognized server request: {}", MessageUtil.getShortOpcodeName(msg));
        break;
    }
  }

  private static void handleConfigChangeNotification(ChannelHandlerContext ctx, ByteBuf message, BucketConfigSink bucketConfigSink) {
    log.debug("{} Received bucket config from server notification", system(ctx.channel()));

    long rev = MessageUtil.getExtras(message).readUnsignedInt();
    String clustermap = MessageUtil.getContentAsString(message);
    InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
    bucketConfigSink.accept(new HostAndPort(remote), clustermap, rev);

    // Couchbase Server does not expect a response to this notification.
  }
}
