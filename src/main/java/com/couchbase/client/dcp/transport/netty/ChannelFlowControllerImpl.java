/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.Channel;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpBufferAckRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.couchbase.client.dcp.message.MessageUtil.requiresFlowControlAck;
import static com.couchbase.client.dcp.transport.netty.DcpPipeline.describe;
import static java.util.Objects.requireNonNull;

public class ChannelFlowControllerImpl implements ChannelFlowController {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelFlowControllerImpl.class);

  private final Channel channel;
  private final String description;
  private final boolean needsBufferAck;
  private final int bufferAckWatermark;

  private int bufferAckCounter;
  private boolean hasLoggedMessageAboutSkippingForInactiveChannel;

  public ChannelFlowControllerImpl(Channel channel, Client.Environment environment) {
    this.channel = requireNonNull(channel);
    this.description = describe(channel).toString();
    this.needsBufferAck = environment.dcpControl().bufferAckEnabled();
    if (needsBufferAck) {
      int bufferAckPercent = environment.bufferAckWatermark();
      int bufferSize = Integer.parseInt(environment.dcpControl().get(DcpControl.Names.CONNECTION_BUFFER_SIZE));
      this.bufferAckWatermark = (int) Math.round(bufferSize / 100.0 * bufferAckPercent);
      LOGGER.info("{} BufferAckWatermark absolute is {} ({}%)", description, bufferAckWatermark, bufferAckPercent);
    } else {
      LOGGER.info("{} Flow control disabled", description);
      this.bufferAckWatermark = 0;
    }
    this.bufferAckCounter = 0;
  }

  @Override
  public void ack(ByteBuf message) {
    if (needsBufferAck && requiresFlowControlAck(message)) {
      ack(message.readableBytes());
    }
  }

  @Override
  public void ack(int numBytes) {
    if (!needsBufferAck) {
      return;
    }

    try {
      synchronized (this) {
        bufferAckCounter += numBytes;
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("{} BufferAckCounter is now {}", description, bufferAckCounter);
        }
        if (bufferAckCounter >= bufferAckWatermark) {
          if (!channel.isActive()) {
            String message = "{} Skipping flow control ACK because channel is no longer active.";
            if (!hasLoggedMessageAboutSkippingForInactiveChannel) {
              hasLoggedMessageAboutSkippingForInactiveChannel = true;
              LOGGER.info(message, description);
            } else {
              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(message, description);
              }
            }
          } else {
            final int bytesToAck = bufferAckCounter;
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("{} BufferAckWatermark reached; acking now against the server for {} bytes.",
                  description, bytesToAck);
            }
            ByteBuf buffer = channel.alloc().buffer();
            DcpBufferAckRequest.init(buffer);
            DcpBufferAckRequest.ackBytes(buffer, bytesToAck);
            channel.writeAndFlush(buffer).addListener(future -> {
              if (future.isSuccess()) {
                LOGGER.debug("{} Flow control ACK success, confirmed {} bytes", describe(channel), bytesToAck);
              } else {
                // ACK failure is bad news. If ignored, it can lead to stream deadlock.
                LOGGER.error("{} Flow control ACK failed; closing channel.", description, future.cause());
                channel.close();
              }
            });
          }
          bufferAckCounter = 0;
        }
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("{} Acknowledging {} bytes.", description, numBytes);
        }
      }
    } catch (Throwable t) {
      if (!channel.isActive()) {
        LOGGER.debug("{} Flow control ack failed (channel already closed?)", description, t);
        return;
      }

      LOGGER.error("{} Flow control ACK failed; closing channel.", description, t);
      channel.close();

      if (t instanceof Error) {
        throw (Error) t;
      }
      throw (RuntimeException) t;
    }
  }
}
