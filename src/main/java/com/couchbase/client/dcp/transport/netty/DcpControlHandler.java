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

import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpControlRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

import static com.couchbase.client.dcp.transport.netty.DcpConnectHandler.getServerVersion;

/**
 * Negotiates DCP control flags once connected and removes itself afterwards.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpControlHandler extends ConnectInterceptingHandler<ByteBuf> {

  /**
   * The logger used.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(DcpControlHandler.class);

  /**
   * Stores an iterator over the control settings that need to be negotiated.
   */
  private Iterator<Map.Entry<String, String>> controlSettings;

  /**
   * The control settings provider. Will tell us the appropriate control settings
   * once we discover the the Couchbase Server version.
   */
  private final DcpControl dcpControl;

  /**
   * Create a new dcp control handler.
   *
   * @param dcpControl the options set by the caller which should be negotiated.
   */
  DcpControlHandler(final DcpControl dcpControl) {
    this.dcpControl = dcpControl;
  }

  /**
   * Helper method to walk the iterator and create a new request that defines which control param
   * should be negotiated right now.
   */
  private void negotiate(final ChannelHandlerContext ctx) {
    if (controlSettings.hasNext()) {
      Map.Entry<String, String> setting = controlSettings.next();

      LOGGER.debug("Negotiating DCP Control {}: {}", setting.getKey(), setting.getValue());
      ByteBuf request = ctx.alloc().buffer();
      DcpControlRequest.init(request);
      DcpControlRequest.key(setting.getKey(), request);
      DcpControlRequest.value(Unpooled.copiedBuffer(setting.getValue(), CharsetUtil.UTF_8), request);

      ctx.writeAndFlush(request);
    } else {
      originalPromise().setSuccess();
      ctx.pipeline().remove(this);
      ctx.fireChannelActive();
      LOGGER.debug("Negotiated all DCP Control settings against Node {}", ctx.channel().remoteAddress());
    }
  }

  /**
   * Once the channel becomes active, start negotiating the dcp control params.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    controlSettings = dcpControl.getControls(getServerVersion(ctx.channel())).entrySet().iterator();
    negotiate(ctx);
  }

  /**
   * Since only one feature per req/res can be negotiated, repeat the process once a response comes
   * back until the iterator is complete or a non-success response status is received.
   */
  @Override
  protected void channelRead0(final ChannelHandlerContext ctx, final ByteBuf msg) throws Exception {
    ResponseStatus status = MessageUtil.getResponseStatus(msg);
    if (status.isSuccess()) {
      negotiate(ctx);
    } else {
      originalPromise().setFailure(new IllegalStateException("Could not configure DCP Controls: " + status));
    }
  }

}
