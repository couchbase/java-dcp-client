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

import com.couchbase.client.dcp.Credentials;
import com.couchbase.client.dcp.core.endpoint.kv.AuthenticationException;
import com.couchbase.client.dcp.core.security.sasl.CouchbaseSaslClientFactory;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import com.couchbase.client.dcp.message.SaslAuthRequest;
import com.couchbase.client.dcp.message.SaslAuthResponse;
import com.couchbase.client.dcp.message.SaslListMechsRequest;
import com.couchbase.client.dcp.message.SaslListMechsResponse;
import com.couchbase.client.dcp.message.SaslStepRequest;
import com.couchbase.client.dcp.message.SaslStepResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import java.io.IOException;
import java.util.Arrays;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.system;
import static com.couchbase.client.dcp.message.ResponseStatus.AUTH_CONTINUE;
import static com.couchbase.client.dcp.message.ResponseStatus.AUTH_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Performs SASL authentication against the socket and once complete removes itself.
 */
public class AuthHandler extends ConnectInterceptingHandler<ByteBuf> implements CallbackHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthHandler.class);

  private final Credentials credentials;
  private SaslClient saslClient;
  private String selectedMechanism;

  public AuthHandler(Credentials credentials) {
    this.credentials = requireNonNull(credentials);
  }

  /**
   * Once the channel is active, start the SASL auth negotiation.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    ByteBuf request = ctx.alloc().buffer();
    SaslListMechsRequest.init(request);
    send(ctx, request);
  }

  /**
   * Every time we receive a message as part of the negotiation process,
   * handle it according to the req/res process.
   */
  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
    if (SaslListMechsResponse.is(msg)) {
      handleListMechsResponse(ctx, msg);

    } else if (SaslAuthResponse.is(msg) || SaslStepResponse.is(msg)) {
      handleSaslResponse(ctx, msg);

    } else {
      throw new IllegalStateException("Received unexpected packet during SASL exchange! " + MessageUtil.humanize(msg));
    }
  }

  /**
   * Runs the SASL challenge protocol and dispatches the next step if required.
   */
  private void handleSaslResponse(final ChannelHandlerContext ctx, final ByteBuf msg) {
    try {
      ResponseStatus status = MessageUtil.getResponseStatus(msg);

      if (status.isSuccess()) {
        if (!saslClient.isComplete()) {
          // verify server signature
          byte[] serverFinal = MessageUtil.getContentAsByteArray(msg);
          saslClient.evaluateChallenge(serverFinal);
          if (!saslClient.isComplete()) {
            throw new IllegalStateException("SASL exchange incomplete");
          }
        }

        LOGGER.debug("Successfully authenticated against node {}", ctx.channel().remoteAddress());
        ctx.pipeline().remove(this);
        originalPromise().setSuccess();
        ctx.fireChannelActive();

      } else if (status == AUTH_CONTINUE) {
        byte[] challenge = MessageUtil.getContentAsByteArray(msg);
        byte[] challengeResponse = saslClient.evaluateChallenge(challenge);

        ByteBuf request = ctx.alloc().buffer();
        SaslStepRequest.init(request);
        SaslStepRequest.mechanism(selectedMechanism, request);
        SaslStepRequest.challengeResponse(challengeResponse, request);
        send(ctx, request);

      } else if (status == AUTH_ERROR) {
        // Bad credentials
        throw new AuthenticationException("SASL Authentication Failure");

      } else {
        throw new AuthenticationException("Unhandled SASL auth status: " + status);
      }

    } catch (Throwable t) {
      fail(t);
    }
  }

  /**
   * Parses the list of supported SASL mechanisms and dispatches the initial auth request.
   */
  private void handleListMechsResponse(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
    try {
      String remote = ctx.channel().remoteAddress().toString();
      String[] supportedMechanisms = SaslListMechsResponse.supportedMechs(msg);
      if (supportedMechanisms.length == 0) {
        throw new AuthenticationException("Received empty SASL mechanisms list from server: " + system(remote));
      }

      saslClient = new CouchbaseSaslClientFactory().createSaslClient(supportedMechanisms, null, "couchbase", remote, null, this);
      if (saslClient == null) {
        throw new AuthenticationException(
            "Failed to create a SASL client for any of the negotiated mechanisms: "
                + Arrays.toString(supportedMechanisms));
      }

      selectedMechanism = saslClient.getMechanismName();
      LOGGER.debug("Selected SASL mechanism: {}", selectedMechanism);

      byte[] payload = saslClient.hasInitialResponse()
          ? saslClient.evaluateChallenge(new byte[0])
          : new byte[0];

      ByteBuf request = ctx.alloc().buffer();
      SaslAuthRequest.init(request);
      SaslAuthRequest.mechanism(selectedMechanism, request);
      SaslAuthRequest.challengeResponse(payload, request);
      send(ctx, request);

    } catch (Throwable t) {
      fail(t);
    }
  }

  /**
   * Handles the SASL defined callbacks to set user and password (the {@link CallbackHandler} interface).
   */
  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        ((NameCallback) callback).setName(credentials.getUsername());
      } else if (callback instanceof PasswordCallback) {
        ((PasswordCallback) callback).setPassword(credentials.getPassword().toCharArray());
      } else {
        throw new AuthenticationException("SASLClient requested unsupported callback: " + callback);
      }
    }
  }

  private void fail(Throwable cause) {
    originalPromise().setFailure(cause);
  }

  private void send(ChannelHandlerContext ctx, ByteBuf request) {
    ctx.writeAndFlush(request).addListener(future -> {
      if (!future.isSuccess()) {
        LOGGER.warn("Error during SASL Auth negotiation phase.", future.cause());
        fail(future.cause());
      }
    });
  }

}
