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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.Client;

public interface ChannelFlowController {
  /**
   * Acknowledge bytes read if DcpControl.Names.CONNECTION_BUFFER_SIZE is set on bootstrap.
   * <p>
   * Note that acknowledgement will be stored but most likely not sent to the server immediately to save network
   * overhead. Instead, depending on the value set through {@link Client.Builder#bufferAckWatermark(int)} in percent
   * the client will automatically determine when to send the message (when the watermark is reached).
   * <p>
   * This method can always be called even if not enabled, if not enabled on bootstrap it will short-circuit.
   *
   * @param message the buffer to acknowledge.
   */
  void ack(ByteBuf message);

  void ack(int numBytes);

  /**
   * A flow controller that doesn't do anything.
   */
  ChannelFlowController dummy = new ChannelFlowController() {
    @Override
    public void ack(ByteBuf message) {
    }

    @Override
    public void ack(int numBytes) {

    }
  };
}
