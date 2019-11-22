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
package com.couchbase.client.dcp;

import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

/**
 * This interface acts as a callback on the {@link Client#controlEventHandler(ControlEventHandler)} API
 * that allows one to react to control events.
 * <p>
 * Right now the only event emitted is a {@link com.couchbase.client.dcp.message.RollbackMessage} which
 * should be handled appropriately since it indicates that very likely the current consumer state is
 * ahead of the server. This happens during failover scenarios and/or if something weird happened
 * to the persisted session state.
 * <p>
 * Keep in mind that the callback is called on the IO event loops, so you should never block or run
 * expensive computations in the callback! Use queues and other synchronization primitives!
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public interface ControlEventHandler {

  /**
   * Called every time when a control event happens that should be handled by
   * consumers of the {@link Client}.
   * <p>
   * Even if you are not doing anything with the events, make sure to release the buffer!!
   *
   * @param flowController the flow controller for the passed event.
   * @param event the control event happening.
   */
  void onEvent(ChannelFlowController flowController, ByteBuf event);
}
