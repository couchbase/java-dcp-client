/*
 * Copyright 2018 Couchbase, Inc.
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

package com.couchbase.client.dcp.buffer;

import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.dcp.transport.netty.DcpResponse;
import com.couchbase.client.dcp.transport.netty.DcpResponseListener;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;

/**
 * Sends DCP requests to the server.
 */
public interface DcpRequestDispatcher {
  /**
   * Assigns a value to the request's {@code opaque} field and writes
   * the message to the channel. Returns a Future that is completed
   * when the response is received or the channel is closed.
   * <p>
   * If the response is received, the Future is always considered
   * successful regardless of the status code returned by the server;
   * the caller is responsible for inspecting the status code.
   * <p>
   * If the channel is not currently active, or if the channel is closed
   * before the response is received, the Future fails with
   * {@link NotConnectedException} as the cause.
   * <p>
   * Callers are responsible for releasing the ByteBuf from successful
   * Futures. This is true even if a call to {@code Future.get} or
   * {@code Future.await} times out, in which case the caller should add
   * a listener to release the buffer when the Future eventually completes.
   * <p>
   * Listeners are invoked by the channel's event loop thread, so they
   * should return quickly.
   * <p>
   * Callers may wish to use the type alias {@link DcpResponseListener}
   * when adding listeners.
   */
  Future<DcpResponse> sendRequest(ByteBuf message);
}
