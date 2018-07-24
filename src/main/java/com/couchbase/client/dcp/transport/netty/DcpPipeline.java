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
import com.couchbase.client.dcp.Credentials;
import com.couchbase.client.dcp.buffer.PersistencePollingHandler;
import com.couchbase.client.dcp.conductor.ConfigProvider;
import com.couchbase.client.dcp.conductor.DcpChannelControlHandler;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.config.SSLEngineFactory;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;
import com.couchbase.client.deps.io.netty.handler.ssl.SslHandler;
import com.couchbase.client.deps.io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;

import static com.couchbase.client.core.lang.backport.java.util.Objects.requireNonNull;

/**
 * Sets up the pipeline for the actual DCP communication channels.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpPipeline extends ChannelInitializer<Channel> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpPipeline.class);

    /**
     * The stateful environment.
     */
    private final ClientEnvironment environment;

    /**
     * The observable where all the control events are fed into for advanced handling up the stack.
     */
    private final DcpChannelControlHandler controlHandler;
    private final SSLEngineFactory sslEngineFactory;
    private final ConfigProvider configProvider;

    /**
     * Creates the pipeline.
     *
     * @param environment
     *            the stateful environment.
     * @param address 
     * @param controlHandler
     *            the control event handler.
     */
    public DcpPipeline(final ClientEnvironment environment,
            final DcpChannelControlHandler controlHandler, ConfigProvider configProvider) {
        this.configProvider = requireNonNull(configProvider);
        this.environment = requireNonNull(environment);
        this.controlHandler = requireNonNull(controlHandler);
        if (environment.sslEnabled()) {
            this.sslEngineFactory = new SSLEngineFactory(environment);
        } else {
            this.sslEngineFactory = null;
        }
    }

    /**
     * Initializes the full pipeline with all handlers needed (some of them may remove themselves during
     * steady state, like auth and feature negotiation).
     */
    @Override
    protected void initChannel(final Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        if (environment.sslEnabled()) {
            pipeline.addLast(new SslHandler(sslEngineFactory.get()));
        }
        pipeline.addLast(
                new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, MessageUtil.BODY_LENGTH_OFFSET, 4, 12, 0, false));

        if (LOGGER.isTraceEnabled()) {
            pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
        }

        DcpControl control = environment.dcpControl();

        Credentials credentials = environment.credentialsProvider().get((InetSocketAddress) ch.remoteAddress());
        pipeline.addLast(new AuthHandler(credentials.getUsername(), credentials.getPassword()))
                .addLast(new DcpConnectHandler(environment.connectionNameGenerator(), environment.bucket(), control))
                .addLast(new DcpControlHandler(control));

        if (control.noopEnabled()) {
            pipeline.addLast(new IdleStateHandler(2 * control.noopIntervalSeconds(), 0, 0));
        }

        DcpMessageHandler messageHandler = new DcpMessageHandler(ch, environment, controlHandler);
        pipeline.addLast(messageHandler);

        if (environment.persistencePollingEnabled()) {
            pipeline.addLast(new PersistencePollingHandler(environment, configProvider, messageHandler));
        }
    }
}
