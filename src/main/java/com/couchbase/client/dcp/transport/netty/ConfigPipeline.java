package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpClientCodec;

public class ConfigPipeline extends ChannelInitializer<Channel> {

    private final String hostname;
    private final String bucket;
    private final String password;

    public ConfigPipeline(String hostname, String bucket, String password) {
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ch.pipeline()
            .addLast(new HttpClientCodec())
            .addLast(new ConfigHandler(hostname, bucket, password));
    }

}
