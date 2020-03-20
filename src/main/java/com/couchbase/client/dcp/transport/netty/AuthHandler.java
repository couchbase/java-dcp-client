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

import com.couchbase.client.dcp.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.dcp.core.security.sasl.Sasl;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.message.SaslAuthRequest;
import com.couchbase.client.dcp.message.SaslAuthResponse;
import com.couchbase.client.dcp.message.SaslListMechsRequest;
import com.couchbase.client.dcp.message.SaslListMechsResponse;
import com.couchbase.client.dcp.message.SaslStepRequest;
import com.couchbase.client.dcp.message.SaslStepResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import java.io.IOException;

import static com.couchbase.client.dcp.message.ResponseStatus.AUTH_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Performs SASL authentication against the socket and once complete removes itself.
 */
class AuthHandler extends ConnectInterceptingHandler<ByteBuf> implements CallbackHandler {

  /**
   * The logger used for the auth handler.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthHandler.class);

  /**
   * Username used to authenticate against the bucket (likely to be the bucket name itself).
   */
  private final String username;

  /**
   * The user/bucket password.
   */
  private final String password;

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
  AuthHandler(String username, String password) {
    this.username = username;
    this.password = password;
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
      checkIsAuthed(ctx, MessageUtil.getResponseStatus(msg));
    } else {
      throw new IllegalStateException("Received unexpected SASL response! " + MessageUtil.humanize(msg));
    }
  }

  /**
   * Runs the SASL challenge protocol and dispatches the next step if required.
   */
  private void handleAuthResponse(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
    if (saslClient.isComplete()) {
      checkIsAuthed(ctx, MessageUtil.getResponseStatus(msg));
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
        content = Unpooled.copiedBuffer(username + "\0" + evaluated[1], UTF_8);
      } else {
        content = Unpooled.wrappedBuffer(evaluatedBytes);
      }

      ByteBuf request = ctx.alloc().buffer();
      SaslStepRequest.init(request);
      SaslStepRequest.mechanism(selectedMechanism, request);
      SaslStepRequest.challengeResponse(content, request);

      ChannelFuture future = ctx.writeAndFlush(request);
      future.addListener(new GenericFutureListener<Future<Void>>() {
        @Override
        public void operationComplete(Future<Void> future) throws Exception {
          if (!future.isSuccess()) {
            LOGGER.warn("Error during SASL Auth negotiation phase.", future);
            originalPromise().setFailure(future.cause());
          }
        }
      });
    } else {
      throw new AuthenticationException("SASL Challenge evaluation returned null.");
    }

  }

  /**
   * Check if the authentication process succeeded or failed based on the response status.
   */
  private void checkIsAuthed(final ChannelHandlerContext ctx, final ResponseStatus status) {
    if (status.isSuccess()) {
      LOGGER.debug("Successfully authenticated against node {}", ctx.channel().remoteAddress());
      ctx.pipeline().remove(this);
      originalPromise().setSuccess();
      ctx.fireChannelActive();

    } else if (status == AUTH_ERROR) {
      originalPromise().setFailure(new AuthenticationException("SASL Authentication Failure"));

    } else {
      originalPromise().setFailure(new AuthenticationException("Unhandled SASL auth status: " + status));
    }
  }

  /**
   * Helper method to parse the list of supported SASL mechs and dispatch the initial auth request following.
   */
  private void handleListMechsResponse(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
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
    SaslAuthRequest.mechanism(selectedMechanism, request);
    SaslAuthRequest.challengeResponse(payload, request);
    payload.release();

    ChannelFuture future = ctx.writeAndFlush(request);
    future.addListener(new GenericFutureListener<Future<Void>>() {
      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        if (!future.isSuccess()) {
          LOGGER.warn("Error during SASL Auth negotiation phase.", future);
          originalPromise().setFailure(future.cause());
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

}
