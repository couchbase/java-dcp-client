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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.conductor.DcpChannelControlHandler;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.message.DcpCloseStreamResponse;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpNoopRequest;
import com.couchbase.client.dcp.message.DcpNoopResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelDuplexHandler;
import com.couchbase.client.deps.io.netty.channel.ChannelHandlerContext;
import com.couchbase.client.deps.io.netty.handler.timeout.IdleState;
import com.couchbase.client.deps.io.netty.handler.timeout.IdleStateEvent;

/**
 * Handles the "business logic" of incoming DCP mutation and control messages.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class DcpMessageHandler extends ChannelDuplexHandler {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpMessageHandler.class);

    /**
     * The data callback where the events are fed to the user.
     */
    private final DataEventHandler dataEventHandler;

    /**
     * The handler for the control events since they need more advanced handling up the stack.
     */
    private final DcpChannelControlHandler controlHandler;

    private final ChannelFlowController flowController;

    /**
     * Create a new message handler.
     *
     * @param environment
     *            data event callback handler.
     * @param controlHandler
     *            control event handler.
     */
    DcpMessageHandler(final Channel channel, final ClientEnvironment environment,
            final DcpChannelControlHandler controlHandler) {
        this.dataEventHandler = environment.dataEventHandler();
        this.controlHandler = controlHandler;
        this.flowController = new ChannelFlowController(channel, environment);
    }

    /**
     * Close dead connection in response to idle event from IdleStateHandler.
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                LOGGER.warn("Closing dead connection.");
                ctx.close();
                return;
            }
        }

        super.userEventTriggered(ctx, evt);
    }

    /**
     * Dispatch every incoming message to either the data or the control feeds.
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;
        if (isDataMessage(message)) {
            dataEventHandler.onEvent(flowController, message);
        } else if (isControlMessage(message)) {
            controlHandler.onEvent(flowController, message);
        } else if (DcpNoopRequest.is(message)) {
            ByteBuf buffer = ctx.alloc().buffer();
            DcpNoopResponse.init(buffer);
            MessageUtil.setOpaque(MessageUtil.getOpaque(message), buffer);
            ctx.writeAndFlush(buffer);
        } else {
            LOGGER.warn("Unknown DCP Message, ignoring. \n{}", MessageUtil.humanize(message));
        }
    }

    /**
     * Helper method to check if the given byte buffer is a control message.
     *
     * @param msg
     *            the message to check.
     * @return true if it is, false otherwise.
     */
    private static boolean isControlMessage(final ByteBuf msg) {
        return DcpOpenStreamResponse.is(msg)
            || DcpStreamEndMessage.is(msg)
            || DcpSnapshotMarkerRequest.is(msg)
            || DcpFailoverLogResponse.is(msg)
            || DcpCloseStreamResponse.is(msg)
            || DcpGetPartitionSeqnosResponse.is(msg);
    }

    /**
     * Helper method to check if the given byte buffer is a data message.
     *
     * @param msg
     *            the message to check.
     * @return true if it is, false otherwise.
     */
    private static boolean isDataMessage(final ByteBuf msg) {
        return DcpMutationMessage.is(msg) || DcpDeletionMessage.is(msg) || DcpExpirationMessage.is(msg);
    }
}
