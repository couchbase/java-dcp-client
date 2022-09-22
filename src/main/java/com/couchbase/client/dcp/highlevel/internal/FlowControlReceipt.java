/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.highlevel.internal;

import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.dcp.message.MessageUtil.requiresFlowControlAck;
import static java.util.Objects.requireNonNull;

public class FlowControlReceipt {
  private final ChannelFlowController flowController;
  private final int ackByteCount;
  private final AtomicBoolean acknowledged = new AtomicBoolean();

  private static final FlowControlReceipt dummy = new FlowControlReceipt(ChannelFlowController.dummy, 0) {
    @Override
    public void acknowledge() {
    }
  };

  public FlowControlReceipt(ChannelFlowController flowController, int ackByteCount) {
    this.flowController = requireNonNull(flowController);
    this.ackByteCount = ackByteCount;
  }

  public void acknowledge() {
    if (acknowledged.compareAndSet(false, true)) {
      flowController.ack(ackByteCount);
    }
  }

  public static FlowControlReceipt forMessage(ChannelFlowController flowController, ByteBuf message) {
    return requiresFlowControlAck(message)
        ? new FlowControlReceipt(flowController, message.readableBytes())
        : dummy;
  }
}
