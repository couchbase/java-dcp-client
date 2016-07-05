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
