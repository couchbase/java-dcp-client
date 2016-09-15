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
import com.couchbase.client.dcp.message.*;
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
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
class AuthHandler
    extends SimpleChannelInboundHandler<ByteBuf>
    implements CallbackHandler, ChannelOutboundHandler {

    /**
     * Indicates a successful SASL auth.
     */
    private static final byte AUTH_SUCCESS = 0x00;

    /**
     * Indicates a failed SASL auth.
     */
    private static final byte AUTH_ERROR = 0x20;

    /**
     * The logger used for the auth handler.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(AuthHandler.class);

    /**
     * Username used to authenticate against the bucket (likely to be the bucket name itself).
     */
    private final String username;

    /**
     * The user/bucket password.
     */
    private final String password;

    /**
     * The original connect promise which is intercepted and then completed/failed after the
     * authentication procedure.
     */
    private ChannelPromise originalPromise;

    /**
     * The SASL client reused from core-io since it has our SCRAM-* additions that are not
     * provided by the vanilla JDK implementation.
     */
    private SaslClient saslClient;

    /**
     * Stores the selected SASL mechanism in the process.
     */
    private String selectedMechanism;

    /**
     * Creates a new auth handler.
     *
     * @param username user/bucket name.
     * @param password password of the user/bucket.
     */
    AuthHandler(final String username, final String password) {
        this.username = username;
        this.password = password == null ? "" : password;
    }

    /**
     * Once the channel is active, start the SASL auth negotiation.
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        ByteBuf request = ctx.alloc().buffer();
        SaslListMechsRequest.init(request);
        ctx.writeAndFlush(request);
    }

    /**
     * Every time we recieve a message as part of the negotiation process, handle
     * it according to the req/res process.
     */
    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        if (SaslListMechsResponse.is(msg)) {
            handleListMechsResponse(ctx, msg);
        } else if (SaslAuthResponse.is(msg)) {
            handleAuthResponse(ctx, msg);
        } else if (SaslStepResponse.is(msg)) {
            checkIsAuthed(ctx, MessageUtil.getStatus(msg));
        } else {
            throw new IllegalStateException("Received unexpected SASL response! " + MessageUtil.humanize(msg));
        }
    }

    /**
     * Runs the SASL challenge protocol and dispatches the next step if required.
     */
    private void handleAuthResponse(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        if (saslClient.isComplete()) {
            checkIsAuthed(ctx, MessageUtil.getStatus(msg));
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
                        originalPromise.setFailure(future.cause());
                    }
                }
            });
        } else {
            throw new AuthenticationException("SASL Challenge evaluation returned null.");
        }

    }

    /**
     * Check if the authenication process suceeded or failed based on the response status.
     */
    private void checkIsAuthed(final ChannelHandlerContext ctx, final short status) {
        switch (status) {
            case AUTH_SUCCESS:
                LOGGER.debug("Successfully authenticated against node {}", ctx.channel().remoteAddress());
                ctx.pipeline().remove(this);
                originalPromise.setSuccess();
                ctx.fireChannelActive();
                break;
            case AUTH_ERROR:
                originalPromise.setFailure(new AuthenticationException("SASL Authentication Failure"));
                break;
            default:
                originalPromise.setFailure(new AuthenticationException("Unhandled SASL auth status: " + status));
        }
    }

    /**
     * Helper method to parse the list of supported SASL mechs and dispatch the initial auth request following.
     */
    private void handleListMechsResponse(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
        String remote = ctx.channel().remoteAddress().toString();
        String[] supportedMechanisms = SaslListMechsResponse.supportedMechs(msg);
        if (supportedMechanisms == null || supportedMechanisms.length == 0) {
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
        payload.release();

        ChannelFuture future = ctx.writeAndFlush(request);
        future.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (!future.isSuccess()) {
                    LOGGER.warn("Error during SASL Auth negotiation phase.", future);
                    originalPromise.setFailure(future.cause());
                }
            }
        });
    }

    /**
     * Handles the SASL defined callbacks to set user and password (the {@link CallbackHandler} interface).
     */
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

    /**
     * Intercept the connect phase and store the original promise.
     */
    @Override
    public void connect(final ChannelHandlerContext ctx, final SocketAddress remoteAddress,
        final SocketAddress localAddress, final ChannelPromise promise) throws Exception {
        originalPromise = promise;
        ChannelPromise inboundPromise = ctx.newPromise();
        inboundPromise.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                if (!future.isSuccess() && !originalPromise.isDone()) {
                    originalPromise.setFailure(future.cause());
                }
            }
        });
        ctx.connect(remoteAddress, localAddress, inboundPromise);
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
