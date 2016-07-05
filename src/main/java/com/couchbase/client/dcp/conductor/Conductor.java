package com.couchbase.client.dcp.conductor;


import com.couchbase.client.dcp.transport.netty.ConfigPipeline;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.oio.OioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.channel.socket.oio.OioSocketChannel;
import rx.Completable;

public class Conductor {

    private String hostname;
    private String bucket;
    private String password;

    public Conductor(String hostname, String bucket, String password) {
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
    }

    public Completable connect() {


        final Bootstrap bootstrap = new Bootstrap()
            .remoteAddress(hostname, 8091)
            .channel(NioSocketChannel.class)
            .handler(new ConfigPipeline(hostname, bucket, password))
            .group(new NioEventLoopGroup());

        bootstrap.connect().awaitUninterruptibly();

        return null;
    }
}
