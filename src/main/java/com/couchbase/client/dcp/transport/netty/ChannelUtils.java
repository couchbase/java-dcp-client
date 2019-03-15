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

import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.deps.io.netty.channel.oio.OioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.channel.socket.oio.OioSocketChannel;

/**
 * Various netty channel related utility methods.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public enum ChannelUtils {
  ;

  /**
   * Helper method to detect the right channel for the given event loop group.
   * <p>
   * Supports Epoll, Nio and Oio.
   *
   * @param group the event loop group passed in.
   * @return returns the right channel class for the group.
   */
  public static Class<? extends Channel> channelForEventLoopGroup(final EventLoopGroup group) {
    Class<? extends Channel> channelClass = NioSocketChannel.class;
    if (group instanceof EpollEventLoopGroup) {
      channelClass = EpollSocketChannel.class;
    } else if (group instanceof OioEventLoopGroup) {
      channelClass = OioSocketChannel.class;
    }
    return channelClass;
  }
}
