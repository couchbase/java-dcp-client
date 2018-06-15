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
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpBufferAckRequest;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSetVbucketStateMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.Channel;

public class ChannelFlowControllerImpl implements ChannelFlowController {
    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ChannelFlowControllerImpl.class);
    /**
     * The DCP channel
     */
    private final Channel channel;
    /**
     * Is ack enabled
     */
    private final boolean needsBufferAck;
    /**
     * Ack water mark
     */
    private final int bufferAckWatermark;
    /**
     * consumed data counter
     */
    private int bufferAckCounter;

    public ChannelFlowControllerImpl(Channel channel, ClientEnvironment environment) {
        this.channel = channel;
        this.needsBufferAck = environment.dcpControl().bufferAckEnabled();
        if (needsBufferAck) {
            int bufferAckPercent = environment.bufferAckWatermark();
            int bufferSize = Integer.parseInt(environment.dcpControl().get(DcpControl.Names.CONNECTION_BUFFER_SIZE));
            this.bufferAckWatermark = (int) Math.round(bufferSize / 100.0 * bufferAckPercent);
            LOGGER.debug("BufferAckWatermark absolute is {}", bufferAckWatermark);
        } else {
            this.bufferAckWatermark = 0;
        }
        this.bufferAckCounter = 0;
    }

    @Override
    public void ack(ByteBuf message) {
        if (needsBufferAck && (DcpSetVbucketStateMessage.is(message) || DcpSnapshotMarkerRequest.is(message)
                || DcpStreamEndMessage.is(message)
                || DcpMutationMessage.is(message) || DcpDeletionMessage.is(message)
                || DcpExpirationMessage.is(message))) {
            ack(message.readableBytes());
        }
    }

    @Override
    public void ack(int numBytes) {
        if (needsBufferAck) {
            synchronized (this) {
                bufferAckCounter += numBytes;
                LOGGER.trace("BufferAckCounter is now {}", bufferAckCounter);
                if (bufferAckCounter >= bufferAckWatermark) {
                    LOGGER.trace("BufferAckWatermark reached on {}, acking now against the server.",
                            channel.remoteAddress());
                    ByteBuf buffer = channel.alloc().buffer();
                    DcpBufferAckRequest.init(buffer);
                    DcpBufferAckRequest.ackBytes(buffer, bufferAckCounter);
                    channel.writeAndFlush(buffer);
                    bufferAckCounter = 0;
                }
                LOGGER.trace("Acknowledging {} bytes against connection {}.", numBytes, channel.remoteAddress());
            }
        }
    }
}
