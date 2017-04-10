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
import com.couchbase.client.dcp.message.DcpCloseStreamResponse;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.DcpStateVbucketStateMessage;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.Channel;

public class ChannelFlowController {
    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(ChannelFlowController.class);
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

    public ChannelFlowController(Channel channel, ClientEnvironment environment) {
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

    /**
     * Acknowledge bytes read if DcpControl.Names.CONNECTION_BUFFER_SIZE is set on bootstrap.
     *
     * Note that acknowledgement will be stored but most likely not sent to the server immediately to save network
     * overhead. Instead, depending on the value set through {@link Builder#bufferAckWatermark(int)} in percent
     * the client will automatically determine when to send the message (when the watermark is reached).
     *
     * This method can always be called even if not enabled, if not enabled on bootstrap it will short-circuit.
     *
     * @param vbid
     *            the partition id.
     * @param numBytes
     *            the number of bytes to acknowledge.
     */
    public void ack(ByteBuf message) {
        if (needsBufferAck && (DcpStateVbucketStateMessage.is(message) || DcpSnapshotMarkerRequest.is(message)
                || DcpStreamEndMessage.is(message) || DcpCloseStreamResponse.is(message)
                || DcpMutationMessage.is(message) || DcpDeletionMessage.is(message)
                || DcpExpirationMessage.is(message))) {
            ack(message.readableBytes());
        }
    }

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
