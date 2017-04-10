/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpCloseStreamResponse;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpGetPartitionSeqnosResponse;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.concurrent.Promise;

public class DcpChannelControlHandler implements ControlEventHandler {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannelControlHandler.class);
    private final DcpChannel dcpChannel;
    private final ControlEventHandler controlEventHandler;
    private final EventBus eventBus;

    public DcpChannelControlHandler(DcpChannel dcpChannel) {
        this.dcpChannel = dcpChannel;
        this.controlEventHandler = dcpChannel.env.controlEventHandler();
        this.eventBus = dcpChannel.env.eventBus();
    }

    @Override
    public void onEvent(ChannelFlowController flowController, ByteBuf buf) {
        if (DcpOpenStreamResponse.is(buf)) {
            filterOpenStreamResponse(flowController, buf);
        } else if (DcpFailoverLogResponse.is(buf)) {
            filterFailoverLogResponse(buf);
        } else if (DcpStreamEndMessage.is(buf)) {
            filterDcpStreamEndMessage(flowController, buf);
        } else if (DcpCloseStreamResponse.is(buf)) {
            filterDcpCloseStreamResponse(flowController, buf);
        } else if (DcpGetPartitionSeqnosResponse.is(buf)) {
            filterDcpGetPartitionSeqnosResponse(buf);
        } else {
            controlEventHandler.onEvent(flowController, buf);
        }
    }

    private boolean filterOpenStreamResponse(ChannelFlowController flowController, ByteBuf buf) {
        try {
            Promise<?> promise = dcpChannel.outstandingPromises.remove(MessageUtil.getOpaque(buf));
            short vbid = dcpChannel.outstandingVbucketInfos.remove(MessageUtil.getOpaque(buf));
            short status = MessageUtil.getStatus(buf);
            switch (status) {
                case 0x00:
                    promise.setSuccess(null);
                    // create a failover log message and emit
                    ByteBuf flog = Unpooled.buffer();
                    DcpFailoverLogResponse.init(flog);
                    DcpFailoverLogResponse.vbucket(flog, DcpOpenStreamResponse.vbucket(buf));
                    ByteBuf content = MessageUtil.getContent(buf).copy().writeShort(vbid);
                    MessageUtil.setContent(content, flog);
                    content.release();
                    controlEventHandler.onEvent(flowController, buf);
                    break;
                case 0x23:
                    promise.setFailure(new RollbackException());
                    // create a rollback message and emit
                    ByteBuf rb = Unpooled.buffer();
                    RollbackMessage.init(rb, vbid, DcpOpenStreamResponse.rollbackSeqno(buf));
                    controlEventHandler.onEvent(flowController, rb);
                    break;
                case 0x07:
                    promise.setFailure(new NotMyVbucketException());
                    break;
                default:
                    promise.setFailure(new IllegalStateException("Unhandled Status: " + status));
            }
            return false;
        } finally {
            buf.release();
        }
    }

    @SuppressWarnings("unchecked")
    private void filterDcpGetPartitionSeqnosResponse(ByteBuf buf) {
        try {
            Promise<ByteBuf> promise =
                    (Promise<ByteBuf>) dcpChannel.outstandingPromises.remove(MessageUtil.getOpaque(buf));
            promise.setSuccess(MessageUtil.getContent(buf).copy());
        } finally {
            buf.release();
        }
    }

    @SuppressWarnings("unchecked")
    private void filterFailoverLogResponse(ByteBuf buf) {
        try {
            Promise<ByteBuf> promise =
                    (Promise<ByteBuf>) dcpChannel.outstandingPromises.remove(MessageUtil.getOpaque(buf));
            short vbid = dcpChannel.outstandingVbucketInfos.remove(MessageUtil.getOpaque(buf));
            ByteBuf flog = Unpooled.buffer();
            DcpFailoverLogResponse.init(flog);
            DcpFailoverLogResponse.vbucket(flog, DcpFailoverLogResponse.vbucket(buf));
            ByteBuf copiedBuf = MessageUtil.getContent(buf).copy().writeShort(vbid);
            MessageUtil.setContent(copiedBuf, flog);
            copiedBuf.release();
            promise.setSuccess(flog);
        } finally {
            buf.release();
        }
    }

    private void filterDcpStreamEndMessage(ChannelFlowController flowController, ByteBuf buf) {
        try {
            short vbid = DcpStreamEndMessage.vbucket(buf);
            StreamEndReason reason = DcpStreamEndMessage.reason(buf);
            LOGGER.debug("Server closed Stream on vbid {} with reason {}", vbid, reason);
            if (eventBus != null) {
                eventBus.publish(new StreamEndEvent(vbid, reason));
            }
            dcpChannel.openStreams.set(vbid, 0);
            dcpChannel.conductor.maybeMovePartition(vbid);
            flowController.ack(buf);
        } finally {
            buf.release();
        }
    }

    private void filterDcpCloseStreamResponse(ChannelFlowController flowController, ByteBuf buf) {
        try {
            Promise<?> promise = dcpChannel.outstandingPromises.remove(MessageUtil.getOpaque(buf));
            promise.setSuccess(null);
            flowController.ack(buf);
        } finally {
            buf.release();
        }
    }
}
