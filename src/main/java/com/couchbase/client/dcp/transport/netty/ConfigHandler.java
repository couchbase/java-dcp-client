package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.channel.SimpleChannelInboundHandler;
import com.couchbase.client.deps.io.netty.handler.codec.base64.Base64;
import com.couchbase.client.deps.io.netty.handler.codec.http.*;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;


public class ConfigHandler extends SimpleChannelInboundHandler<HttpObject> {

    private final String hostname;
    private final String bucket;
    private final String password;
    private ByteBuf responseContent;

    public ConfigHandler(String hostname, String bucket, String password) {
        this.hostname = hostname;
        this.bucket = bucket;
        this.password = password;
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

            // TODO: emit into subject here.

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
