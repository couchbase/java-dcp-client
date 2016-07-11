package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.deps.io.netty.channel.oio.OioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.channel.socket.oio.OioSocketChannel;

/**
 * Created by daschl on 10/07/16.
 */
public enum ChannelUtils {
    ;

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
