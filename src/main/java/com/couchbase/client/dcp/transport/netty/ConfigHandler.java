/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpContent;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpObject;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import rx.subjects.Subject;

/**
 * This handler is responsible to consume chunks of JSON configs via HTTP, aggregate them and once a complete
 * config is received send it into a {@link Subject} for external usage.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
class ConfigHandler extends SimpleChannelInboundHandler<HttpObject> {

    /**
     * Hostname used to replace $HOST parts in the config when used against localhost.
     */
    private final String hostname;

    /**
     * The config stream where the configs are emitted into.
     */
    private final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream;
    private final ClientEnvironment environment;

    /**
     * The current aggregated chunk of the JSON config.
     */
    private ByteBuf responseContent;

    /**
     * Creates a new config handler.
     *
     * @param hostname     hostname of the remote server.
     * @param configStream config stream where to send the configs.
     * @param environment  the environment.
     */
    ConfigHandler(final String hostname,
                  final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream, ClientEnvironment environment) {
        this.hostname = hostname;
        this.configStream = configStream;
        this.environment = environment;
    }

    /**
     * If we get a new content chunk, send it towards decoding.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final HttpObject msg) throws Exception {
        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            decodeChunk(content.content());
        }
    }

    /**
     * Helper method to decode and analyze the chunk.
     *
     * @param chunk the chunk to analyze.
     */
    private void decodeChunk(final ByteBuf chunk) {
        responseContent.writeBytes(chunk);

        String currentChunk = responseContent.toString(CharsetUtil.UTF_8);
        int separatorIndex = currentChunk.indexOf("\n\n\n\n");
        if (separatorIndex > 0) {
            String rawConfig = currentChunk
                    .substring(0, separatorIndex)
                    .trim()
                    .replace("$HOST", hostname);

            configStream.onNext((CouchbaseBucketConfig) BucketConfigParser.parse(rawConfig, environment));
            responseContent.clear();
            responseContent.writeBytes(currentChunk.substring(separatorIndex + 4).getBytes(CharsetUtil.UTF_8));
        }
    }

    /**
     * Once the handler is added, initialize the response content buffer.
     */
    @Override
    public void handlerAdded(final ChannelHandlerContext ctx) throws Exception {
        responseContent = ctx.alloc().buffer();
    }

    /**
     * Once the handler is removed, make sure the response content is released and freed.
     */
    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        if (responseContent != null && responseContent.refCnt() > 0) {
            responseContent.release();
            responseContent = null;
        }
    }
}
