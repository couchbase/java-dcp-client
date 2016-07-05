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

import com.couchbase.client.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.security.sasl.Sasl;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.internal.*;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.*;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;

import javax.security.auth.callback.*;
import javax.security.sasl.SaslClient;
import java.io.IOException;
import java.net.SocketAddress;

/**
 * Performs SASL authentication against the socket and once complete removes itself.
 */
public class AuthHandler
    extends SimpleChannelInboundHandler<ByteBuf>
    implements CallbackHandler, ChannelOutboundHandler {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(AuthHandler.class);

    private final String username;
    private final String password;
    private ChannelHandlerContext ctx;
    private ChannelPromise originalPromise;
    private SaslClient saslClient;
    private String selectedMechanism;

    public AuthHandler(String username, String password) {
        this.username = username;
        this.password = password == null ? "" : password;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
        ByteBuf request = ctx.alloc().buffer();
        SaslListMechsRequest.init(request);
        ctx.writeAndFlush(request);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        if (SaslListMechsResponse.is(msg)) {
            handleListMechsResponse(ctx, msg);
        } else if (SaslAuthResponse.is(msg)) {
            handleAuthResponse(ctx, msg);
        } else if (SaslStepResponse.is(msg)) {
            checkIsAuthed(MessageUtil.getStatus(msg));
        }
    }

    private void handleAuthResponse(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        if (saslClient.isComplete()) {
            checkIsAuthed(MessageUtil.getStatus(msg));
            return;
        }

        ByteBuf challengeBuf = SaslAuthResponse.challenge(msg);
        byte[] challenge = new byte[challengeBuf.readableBytes()];
        challengeBuf.readBytes(challenge);
        byte[] evaluatedBytes = saslClient.evaluateChallenge(challenge);

        if (evaluatedBytes != null) {
            ByteBuf content;

            // This is needed against older server versions where the protocol does not
            // align on cram and plain, the else block is used for all the newer cram-sha*
            // mechanisms.
            //
            // Note that most likely this is only executed in the CRAM-MD5 case only, but
            // just to play it safe keep it for both mechanisms.
            if (selectedMechanism.equals("CRAM-MD5") || selectedMechanism.equals("PLAIN")) {
                String[] evaluated = new String(evaluatedBytes).split(" ");
                content = Unpooled.copiedBuffer(username + "\0" + evaluated[1], CharsetUtil.UTF_8);
            } else {
                content = Unpooled.wrappedBuffer(evaluatedBytes);
            }

            ByteBuf request = ctx.alloc().buffer();
            SaslStepRequest.init(request);
            SaslStepRequest.mechanism(Unpooled.copiedBuffer(selectedMechanism, CharsetUtil.UTF_8), request);
            SaslStepRequest.challengeResponse(content, request);

            ChannelFuture future = ctx.writeAndFlush(request);
            future.addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
                    if (!future.isSuccess()) {
                        LOGGER.warn("Error during SASL Auth negotiation phase.", future);
                    }
                }
            });
        } else {
            throw new AuthenticationException("SASL Challenge evaluation returned null.");
        }

    }

    private void checkIsAuthed(final short status) {
        switch (status) {
            case 0x00: // auth success
                originalPromise.setSuccess();
                ctx.pipeline().remove(this);
                ctx.fireChannelActive();
                LOGGER.debug("Authenticated against Node {}", ctx.channel().remoteAddress());
                break;
            case 0x20: // auth failure
                originalPromise.setFailure(new AuthenticationException("Authentication Failure"));
                break;
            default:
                originalPromise.setFailure(new AuthenticationException("Unhandled SASL auth status: " + status));
        }
    }

    private void handleListMechsResponse(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        String remote = ctx.channel().remoteAddress().toString();
        String[] supportedMechanisms = SaslListMechsResponse.supportedMechs(msg);
        if (supportedMechanisms.length == 0) {
            throw new AuthenticationException("Received empty SASL mechanisms list from server: " + remote);
        }

        saslClient = Sasl.createSaslClient(supportedMechanisms, null, "couchbase", remote, null, this);
        selectedMechanism = saslClient.getMechanismName();
        byte[] bytePayload = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[]{}) : null;
        ByteBuf payload = bytePayload != null ? ctx.alloc().buffer().writeBytes(bytePayload) : Unpooled.EMPTY_BUFFER;

        ByteBuf request = ctx.alloc().buffer();
        SaslAuthRequest.init(request);
        SaslAuthRequest.mechanism(Unpooled.copiedBuffer(selectedMechanism, CharsetUtil.UTF_8), request);
        SaslAuthRequest.challengeResponse(payload, request);

        ChannelFuture future = ctx.writeAndFlush(request);
        future.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (!future.isSuccess()) {
                    LOGGER.warn("Error during SASL Auth negotiation phase.", future);
                }
            }
        });
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                ((NameCallback) callback).setName(username);
            } else if (callback instanceof PasswordCallback) {
                ((PasswordCallback) callback).setPassword(password.toCharArray());
            } else {
                throw new AuthenticationException("SASLClient requested unsupported callback: " + callback);
            }
        }
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                        ChannelPromise promise) throws Exception {
            originalPromise = promise;
            ChannelPromise downPromise = ctx.newPromise();
            downPromise.addListener(new GenericFutureListener<Future<Void>>() {
                @Override
                public void operationComplete(Future<Void> future) throws Exception {
                    if (!future.isSuccess() && !originalPromise.isDone()) {
                        originalPromise.setFailure(future.cause());
                    }
                }
            });
            ctx.connect(remoteAddress, localAddress, downPromise);
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        ctx.bind(localAddress, promise);
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.disconnect(promise);
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.close(promise);
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        ctx.deregister(promise);
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        ctx.read();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ctx.read();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

}
