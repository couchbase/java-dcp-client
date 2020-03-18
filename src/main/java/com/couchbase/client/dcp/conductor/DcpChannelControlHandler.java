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

import com.couchbase.client.dcp.core.event.EventBus;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpStreamEndMessage;
import com.couchbase.client.dcp.message.StreamEndReason;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DcpChannelControlHandler implements ControlEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DcpChannelControlHandler.class);
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
    if (DcpStreamEndMessage.is(buf)) {
      filterDcpStreamEndMessage(flowController, buf);
    } else {
      controlEventHandler.onEvent(flowController, buf);
    }
  }

  private void filterDcpStreamEndMessage(ChannelFlowController flowController, ByteBuf buf) {
    try {
      final short vbid = DcpStreamEndMessage.vbucket(buf);
      final StreamEndReason reason = DcpStreamEndMessage.reason(buf);
      LOGGER.debug("Server closed Stream on vbid {} with reason {}", vbid, reason);

      final StreamEndEvent event = new StreamEndEvent(vbid, reason);

      if (dcpChannel.env.persistencePollingEnabled()) {
        // stream event buffer will publish event at appropriate time
        dcpChannel.env.streamEventBuffer().onStreamEnd(event);
      } else {
        eventBus.publish(event);
      }

      dcpChannel.streamIsOpen.set(vbid, false);
      if (reason != StreamEndReason.OK) {
        dcpChannel.conductor.maybeMovePartition(vbid);
      }
    } finally {
      try {
        flowController.ack(buf);
      } finally {
        buf.release();
      }
    }
  }
}
