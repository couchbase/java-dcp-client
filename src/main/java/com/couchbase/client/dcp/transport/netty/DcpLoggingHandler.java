/*
 * Copyright 2018 Couchbase, Inc.
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
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufHolder;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandler;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandler.Sharable;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.core.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.core.deps.io.netty.handler.logging.LoggingHandler;

/**
 * A {@link ChannelHandler} that logs all events using a logging framework.
 * By default, all events are logged at {@code DEBUG} level.
 */
@Sharable
public class DcpLoggingHandler extends LoggingHandler {

  /**
   * Creates a new instance whose logger name is the fully qualified class
   * name of the instance with hex dump enabled.
   */
  public DcpLoggingHandler() {
    super();
  }

  /**
   * Creates a new instance whose logger name is the fully qualified class
   * name of the instance.
   *
   * @param level the log level
   */
  public DcpLoggingHandler(LogLevel level) {
    super(level);
  }

  /**
   * Creates a new instance with the specified logger name and with hex dump
   * enabled.
   */
  public DcpLoggingHandler(Class<?> clazz) {
    super(clazz);
  }

  /**
   * Creates a new instance with the specified logger name.
   *
   * @param level the log level
   */
  public DcpLoggingHandler(Class<?> clazz, LogLevel level) {
    super(clazz, level);
  }

  /**
   * Creates a new instance with the specified logger name.
   */
  public DcpLoggingHandler(String name) {
    super(name);
  }

  /**
   * Creates a new instance with the specified logger name.
   *
   * @param level the log level
   */
  public DcpLoggingHandler(String name, LogLevel level) {
    super(name, level);
  }

  @Override
  protected String format(ChannelHandlerContext ctx, String eventName, Object arg) {
    if (arg instanceof ByteBuf) {
      return formatDcpPacket(ctx, eventName, (ByteBuf) arg);
    } else if (arg instanceof ByteBufHolder) {
      return formatDcpPacket(ctx, eventName, ((ByteBufHolder) arg).content());
    } else {
      return super.format(ctx, eventName, arg);
    }
  }

  /**
   * Returns a String which contains all details to log the {@link ByteBuf}
   */
  private String formatDcpPacket(ChannelHandlerContext ctx, String eventName, ByteBuf msg) {
    if (msg.readableBytes() == 0) {
      return ctx.channel() + " " + eventName + ": 0B";
    }
    return ctx.channel() + " " + eventName + ":\n" + MessageUtil.humanize(msg);
  }
}

