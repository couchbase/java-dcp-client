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

import com.couchbase.client.dcp.core.CouchbaseException;
import com.couchbase.client.dcp.core.logging.RedactableArgument;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;

/**
 * This handler intercepts the bootstrap of the config stream, sending the initial request
 * and checking the response for potential errors.
 */
class StartStreamHandler extends ConnectInterceptingHandler<HttpResponse> {

  private final String bucket;
  private final String username;
  private final String password;

  public StartStreamHandler(String bucket, String username, String password) {
    this.bucket = bucket;
    this.username = username;
    this.password = password;
  }

  /**
   * Once the channel is active, start to send the HTTP request to begin chunking.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    String terseUri = "/pools/default/bs/" + bucket;
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, terseUri);
    request.headers().add(HttpHeaders.Names.ACCEPT, "application/json");
    addHttpBasicAuth(ctx, request);
    ctx.writeAndFlush(request);
  }

  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final HttpResponse msg) throws Exception {
    int statusCode = msg.getStatus().code();
    if (statusCode == 200) {
      ctx.pipeline().remove(this);
      originalPromise().setSuccess();
      ctx.fireChannelActive();
    } else {
      CouchbaseException exception;
      switch (statusCode) {
        case 401:
          exception = new CouchbaseException("Unauthorized (bucket/password invalid) - please check credentials!");
          break;
        case 404:
          exception = new CouchbaseException("Got HTTP status code 404 (Not Found)." +
              " Does bucket '" + RedactableArgument.meta(bucket) + "' exist?");
          break;
        default:
          exception = new CouchbaseException("Unknown error code during connect: " + msg.getStatus());

      }
      originalPromise().setFailure(exception);
    }
  }

  /**
   * Helper method to add authentication credentials to the config stream request.
   */
  private void addHttpBasicAuth(final ChannelHandlerContext ctx, final HttpRequest request) {
    ByteBuf raw = ctx.alloc().buffer(username.length() + password.length() + 1);
    raw.writeBytes((username + ":" + password).getBytes(CharsetUtil.UTF_8));
    ByteBuf encoded = Base64.encode(raw, false);
    request.headers().add(HttpHeaders.Names.AUTHORIZATION, "Basic " + encoded.toString(CharsetUtil.UTF_8));
    encoded.release();
    raw.release();
  }

}
