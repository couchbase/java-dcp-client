/*
 * Copyright 2019 Couchbase, Inc.
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

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.couchbase.client.dcp.message.MessageUtil.NOOP_OPCODE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Issues ordinary NOOP requests to the server when the DCP channel is idle.
 * Useful if the channel is not used for streaming, since the server only sends
 * DCP_NOOP requests to the client when there is at least one stream open.
 */
public class ClientNoopHandler extends IdleStateHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientNoopHandler.class);

  public ClientNoopHandler(long readerIdleTime, TimeUnit unit) {
    super(readerIdleTime, 0, 0, unit);
  }

  @Override
  protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
    LOGGER.debug("Nothing read from channel {} for {} seconds; sending client-side NOOP request.",
        ctx.channel(), MILLISECONDS.toSeconds(getReaderIdleTimeInMillis()));

    ctx.pipeline().get(DcpMessageHandler.class)
        .sendRequest(newNoopRequest())
        .addListener((DcpResponseListener) future -> {
          if (future.isSuccess()) {
            final DcpResponse dcpResponse = future.getNow();
            final ByteBuf buffer = dcpResponse.buffer();
            try {
              final ResponseStatus status = dcpResponse.status();
              if (status.isSuccess()) {
                LOGGER.debug("Got successful response to client-side NOOP for channel {}", ctx.channel());
              } else {
                LOGGER.warn("Got error response to client-side NOOP for channel {}: {}", ctx.channel(), status);
              }
            } finally {
              buffer.release();
            }
          } else {
            LOGGER.warn("Failed to send client-side NOOP for channel {}", ctx.channel(), future.cause());
          }
        });
  }

  private static ByteBuf newNoopRequest() {
    final ByteBuf buffer = Unpooled.buffer();
    MessageUtil.initRequest(NOOP_OPCODE, buffer);
    return buffer;
  }
}
