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
import com.couchbase.client.dcp.message.*;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.*;
import rx.subjects.Subject;

public class DcpMessageHandler extends ChannelDuplexHandler {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpMessageHandler.class);

    private final DataEventHandler dataEventHandler;
    private final Subject<ByteBuf, ByteBuf> controlEvents;

    public DcpMessageHandler(DataEventHandler dataEventHandler, Subject<ByteBuf, ByteBuf> controlEvents) {
        this.dataEventHandler = dataEventHandler;
        this.controlEvents = controlEvents;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        if (isDataMessage(message)) {
            dataEventHandler.onEvent(message);
        } else if (isControlMessage(message)) {
            controlEvents.onNext(message);
        } else if (DcpNoopRequest.is(message)) {
            ByteBuf buffer = ctx.alloc().buffer();
            DcpNoopResponse.init(buffer);
            MessageUtil.setOpaque(MessageUtil.getOpaque(message), buffer);
            ctx.writeAndFlush(buffer);
        } else {
            LOGGER.warn("Unknown DCP Message, ignoring. \n{}", MessageUtil.humanize(message));
        }
    }

    private static boolean isControlMessage(ByteBuf msg) {
        return DcpOpenStreamResponse.is(msg)
            || DcpStreamEndMessage.is(msg)
            || DcpSnapshotMarkerMessage.is(msg)
            || DcpFailoverLogResponse.is(msg)
            || DcpCloseStreamResponse.is(msg)
            || DcpGetPartitionSeqnosResponse.is(msg);
    }

    private static boolean isDataMessage(ByteBuf msg) {
        return DcpMutationMessage.is(msg)
            || DcpDeletionMessage.is(msg)
            || DcpExpirationMessage.is(msg);
    }

}
