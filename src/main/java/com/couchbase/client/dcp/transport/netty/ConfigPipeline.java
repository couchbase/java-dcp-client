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

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;
import rx.subjects.Subject;

/**
 * Configures the pipeline for the HTTP config stream.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class ConfigPipeline extends ChannelInitializer<Channel> {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ConfigPipeline.class);

    /**
     * Hostname used to replace $HOST parts in the config when used against localhost.
     */
    private final String hostname;

    /**
     * The name of the bucket (used for http auth).
     */
    private final String bucket;

    /**
     * THe password of the bucket (used for http auth).
     */
    private final String password;

    /**
     * The config stream where the configs are emitted into.
     */
    private final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream;

    /**
     * Creates a new config pipeline.
     *
     * @param hostname hostname of the remote server.
     * @param bucket name of the bucket/user.
     * @param password password of the bucket/user.
     * @param configStream config stream where to send the configs.
     */
    public ConfigPipeline(final String hostname, final String bucket, final String password,
        final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream) {
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
        this.configStream = configStream;
    }

    /**
     * Init the pipeline with the HTTP codec and our handler which does all the json chunk
     * decoding and parsing.
     *
     * If trace logging is enabled also add the logging handler so we can figure out whats
     * going on when debugging.
     */
    @Override
    protected void initChannel(final Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        if (LOGGER.isTraceEnabled()) {
            pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
        }

        pipeline
            .addLast(new HttpClientCodec())
            .addLast(new ConfigHandler(hostname, bucket, password, configStream));
    }

}
