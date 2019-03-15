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
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.ByteBufHolder;
import com.couchbase.client.deps.io.netty.channel.ChannelHandler;
import com.couchbase.client.deps.io.netty.channel.ChannelHandler.Sharable;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;

/**
 * A {@link ChannelHandler} that logs all events using a logging framework.
 * By default, all events are logged at <tt>DEBUG</tt> level.
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

  protected String formatMessage(String eventName, Object msg) {
    if (msg instanceof ByteBuf) {
      return formatByteBuf(eventName, (ByteBuf) msg);
    } else if (msg instanceof ByteBufHolder) {
      return formatByteBufHolder(eventName, (ByteBufHolder) msg);
    } else {
      return formatNonByteBuf(eventName, msg);
    }
  }

  /**
   * Returns a String which contains all details to log the {@link ByteBuf}
   */
  protected String formatByteBuf(String eventName, ByteBuf msg) {
    int length = msg.readableBytes();
    if (length == 0) {
      StringBuilder buf = new StringBuilder(eventName.length() + 4);
      buf.append(eventName).append(": 0B");
      return buf.toString();
    } else {
      return "\n" + MessageUtil.humanize(msg);
    }
  }

  /**
   * Returns a String which contains all details to log the {@link ByteBufHolder}.
   * <p>
   * By default this method just delegates to {@link #formatByteBuf(String, ByteBuf)},
   * using the content of the {@link ByteBufHolder}. Sub-classes may override this.
   */
  protected String formatByteBufHolder(String eventName, ByteBufHolder msg) {
    return formatByteBuf(eventName, msg.content());
  }
}

