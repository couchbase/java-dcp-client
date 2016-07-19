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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;
import rx.subjects.Subject;

public class DcpPipeline extends ChannelInitializer<Channel> {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpPipeline.class);


    private final ClientEnvironment environment;
    private final Subject<ByteBuf, ByteBuf> controlEvents;

    public DcpPipeline(ClientEnvironment environment, Subject<ByteBuf, ByteBuf> controlEvents) {
        this.environment = environment;
        this.controlEvents = controlEvents;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(new LengthFieldBasedFrameDecoder(
            Integer.MAX_VALUE, MessageUtil.BODY_LENGTH_OFFSET, 4, 12, 0, false
        ));

        if (LOGGER.isTraceEnabled()) {
            pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
        }

        pipeline.addLast(new AuthHandler(environment.bucket(), environment.password()))
            .addLast(new DcpConnectHandler(environment.connectionNameGenerator()))
            .addLast(new DcpControlHandler(environment.dcpControl()))
            .addLast(new DcpMessageHandler(environment.dataEventHandler(), controlEvents));
    }
}
