/*
 * Copyright 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.dcp.message.DcpBufferAckRequest;
import com.couchbase.client.dcp.message.DcpControlRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.metrics.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.couchbase.client.dcp.config.DcpControl.Names.CONNECTION_BUFFER_SIZE;
import static java.util.Objects.requireNonNull;

public class FlowControlDiagnosticHandler extends ChannelDuplexHandler {
  private static final Logger log = LoggerFactory.getLogger(FlowControlDiagnosticHandler.class);

  private static final Duration warningThreshold = Duration.ofMinutes(1);

  private final String channelDesc;

  private long flowControlBufferSizeInBytes;
  private long unAckedBytes;
  private ScheduledFuture<?> scheduledTask;
  private NanoTimestamp lastAckTime = NanoTimestamp.now();

  public FlowControlDiagnosticHandler(String channelDesc) {
    this.channelDesc = requireNonNull(channelDesc);
  }

  public void channelActive(ChannelHandlerContext ctx) {
    scheduledTask = ctx.executor().scheduleAtFixedRate(this::logStatus, 10, 10, TimeUnit.SECONDS);
    ctx.fireChannelActive();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (scheduledTask != null) {
      scheduledTask.cancel(true);
    }
    ctx.fireChannelInactive();
  }

  private void logStatus() {
    if (flowControlBufferSizeInBytes > 0) {
      LogLevel logLevel = bufferFull() && lastAckTime.hasElapsed(warningThreshold) ? LogLevel.WARN : LogLevel.DEBUG;
      logLevel.log(log, "{} STATUS: {} ; time since last ack = {}", channelDesc, bufferStatus(), lastAckTime.elapsed());
    }
  }

  private boolean bufferFull() {
    return unAckedBytes >= flowControlBufferSizeInBytes;
  }

  private Object bufferStatus() {
    return new Object() {
      @Override
      public String toString() {
        int percent = (int) Math.ceil(((double) unAckedBytes / (double) flowControlBufferSizeInBytes) * 100);
        String fullOrNot = bufferFull() ? "BUFFER FULL" : "ok";
        return "un-acked bytes = " + unAckedBytes + " / " + flowControlBufferSizeInBytes
            + " (" + percent + "%) " + fullOrNot;
      }
    };
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg)  {
    ByteBuf buf = (ByteBuf) msg;
    if (MessageUtil.requiresFlowControlAck(buf)) {
      int messageSize = buf.readableBytes();
      unAckedBytes += messageSize;
      if (log.isTraceEnabled()) {
        log.trace("{} Received {} flow-controlled bytes; new {}",
            channelDesc,
            messageSize,
            bufferStatus()
        );
      }
    }
    ctx.fireChannelRead(msg);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    ByteBuf buf = (ByteBuf) msg;

    if (DcpControlRequest.is(buf)) {
      if (MessageUtil.getKeyAsString(buf).equals(CONNECTION_BUFFER_SIZE.value())) {
        flowControlBufferSizeInBytes = Long.parseLong(MessageUtil.getContentAsString(buf));
        log.debug("{} Flow control buffer size initialized to {} bytes", channelDesc, flowControlBufferSizeInBytes);
      }
    }

    if (DcpBufferAckRequest.is(buf)) {
      long ackedBytes = MessageUtil.getExtras(buf).readUnsignedInt();

      if (ackedBytes > Integer.MAX_VALUE) {
        log.error("{} Acked byte count {} is > Integer.MAX_VALUE; is that a problem?", channelDesc, ackedBytes);
      }

      if (unAckedBytes < 0) {
        log.error("{} Un-acked byte count {} is < 0", channelDesc, unAckedBytes);
      }

      promise = promise.unvoid();
      promise.addListener(future -> {
        if (future.isSuccess()) {
          lastAckTime = NanoTimestamp.now();
          unAckedBytes -= ackedBytes;
          log.debug("{} Wrote flow control ack for {} bytes; new {}", channelDesc, ackedBytes, bufferStatus());
        } else {
          log.error("{} Failed to write flow control ack for {} bytes; some other component will close this chanel.", channelDesc, ackedBytes);
        }
      });
    }

    ctx.write(msg, promise);
  }

}
