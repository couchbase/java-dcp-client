package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;

public class DcpPipeline extends ChannelInitializer<Channel> {

    private final ConnectionNameGenerator connectionNameGenerator;
    private final String bucket;
    private final String password;
    private final DcpControl dcpControl;

    public DcpPipeline(ConnectionNameGenerator connectionNameGenerator, String bucket, String password, DcpControl dcpControl) {
        this.connectionNameGenerator = connectionNameGenerator;
        this.bucket = bucket;
        this.password = password;
        this.dcpControl = dcpControl;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ch.pipeline()
            .addLast(new LoggingHandler(LogLevel.TRACE))
            .addLast(new AuthHandler(bucket, password))
            .addLast(new DcpConnectHandler(connectionNameGenerator))
            .addLast(new DcpControlHandler(dcpControl));
    }
}
