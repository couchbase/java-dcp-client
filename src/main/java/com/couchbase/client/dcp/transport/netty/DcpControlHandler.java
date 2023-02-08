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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.dcp.config.Control;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpControlRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.ResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.couchbase.client.dcp.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.dcp.message.ResponseStatus.INVALID_ARGUMENTS;
import static com.couchbase.client.dcp.transport.netty.DcpConnectHandler.getServerVersion;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Negotiates DCP controls once connected and removes itself afterwards.
 */
public class DcpControlHandler extends ConnectInterceptingHandler<ByteBuf> {

  private static final Logger log = LoggerFactory.getLogger(DcpControlHandler.class);

  /**
   * Remaining controls to negotiate.
   */
  private Iterator<Control> remainingControls;

  /**
   * Control currently being negotiated.
   */
  private Control currentControl;

  /**
   * Successfully negotiated controls.
   */
  private final List<Control> negotiatedControls = new ArrayList<>();

  /**
   * The control settings provider. Will tell us the appropriate control settings
   * once we discover the Couchbase Server version.
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
    if (remainingControls.hasNext()) {
      currentControl = remainingControls.next();

      log.debug("Negotiating DCP Control {}", currentControl);
      ByteBuf request = ctx.alloc().buffer();
      DcpControlRequest.init(request);
      DcpControlRequest.key(currentControl.name(), request);
      DcpControlRequest.value(Unpooled.copiedBuffer(currentControl.value(), UTF_8), request);

      ctx.writeAndFlush(request);
    } else {
      log.info("DCP control negotiation complete for node {} ; {}",
          redactSystem(ctx.channel().remoteAddress()), negotiatedControls);

      originalPromise().setSuccess();
      ctx.pipeline().remove(this);
      ctx.fireChannelActive();
    }
  }

  /**
   * Once the channel becomes active, start negotiating the dcp control params.
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    remainingControls = dcpControl.getControls(getServerVersion(ctx.channel())).iterator();
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
      negotiatedControls.add(currentControl);
      log.debug("Successfully negotiated DCP control: {}", currentControl);

    } else if (status == INVALID_ARGUMENTS && currentControl.isOptional()) {
      log.debug("Server rejected optional DCP control: {} ; status = {}", currentControl, status);

    } else {
      originalPromise().setFailure(
          new RuntimeException("Failed to negotiate DCP control: " + currentControl + " ; status = " + status)
      );
      return;
    }

    negotiate(ctx);
  }
}
