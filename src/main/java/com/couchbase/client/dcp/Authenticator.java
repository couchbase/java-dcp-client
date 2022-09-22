/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.dcp;

import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.ssl.SslContextBuilder;

public interface Authenticator {

  /**
   * Allows the authenticator to add KV handlers during connection bootstrap to perform
   * authentication.
   *
   * @param pipeline the pipeline when the endpoint is constructed.
   */
  default void authKeyValueConnection(final ChannelPipeline pipeline) {
  }

  /**
   * The authenticator gets the chance to attach the client certificate to the ssl context if needed.
   *
   * @param sslContextBuilder the netty context builder
   */
  default void applyTlsProperties(final SslContextBuilder sslContextBuilder) {
  }

  /**
   * If this authenticator only works with encrypted connections.
   */
  default boolean requiresTls() { return false; }
}
