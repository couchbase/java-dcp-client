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
import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.base64.Base64;
import com.couchbase.client.deps.io.netty.handler.codec.http.*;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import rx.subjects.Subject;


public class ConfigHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final String hostname;
    private final String bucket;
    private final String password;
    private ByteBuf responseContent;
    private final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream;

    public ConfigHandler(String hostname, String bucket, String password,
        Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream) {
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
        this.configStream = configStream;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            decodeChunk(content.content());
        }
    }

    private void decodeChunk(ByteBuf chunk) {
        responseContent.writeBytes(chunk);

        String currentChunk = responseContent.toString(CharsetUtil.UTF_8);
        int separatorIndex = currentChunk.indexOf("\n\n\n\n");
        if (separatorIndex > 0) {
            String rawConfig = currentChunk
                .substring(0, separatorIndex)
                .trim()
                .replace("$HOST", hostname);

            configStream.onNext((CouchbaseBucketConfig) BucketConfigParser.parse(rawConfig));
            responseContent.clear();
            responseContent.writeBytes(currentChunk.substring(separatorIndex + 4).getBytes(CharsetUtil.UTF_8));
        }

    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        responseContent = ctx.alloc().buffer();
        ctx.fireChannelActive();

        FullHttpRequest request = new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            "/pools/default/bs/" + bucket
        );

        request.headers().add(HttpHeaders.Names.ACCEPT, "application/json");
        addHttpBasicAuth(ctx, request, bucket, password);
        ctx.writeAndFlush(request);
    }

    public static void addHttpBasicAuth(final ChannelHandlerContext ctx, final HttpRequest request, final String user,
        final String password) {
        final String pw = password == null ? "" : password;
        ByteBuf raw = ctx.alloc().buffer(user.length() + pw.length() + 1);
        raw.writeBytes((user + ":" + pw).getBytes(CharsetUtil.UTF_8));
        ByteBuf encoded = Base64.encode(raw, false);
        request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
        encoded.release();
        raw.release();
    }
}
