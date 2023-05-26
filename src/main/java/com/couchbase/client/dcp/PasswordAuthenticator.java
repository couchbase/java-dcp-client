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
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.transport.netty.AuthHandler;

import static java.util.Objects.requireNonNull;

public class PasswordAuthenticator implements Authenticator {
  private final CredentialsProvider credentialsProvider;

  public PasswordAuthenticator(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = requireNonNull(credentialsProvider);
  }

  @Override
  public void authKeyValueConnection(ChannelPipeline pipeline) {
    HostAndPort remoteAddress = DcpChannel.getHostAndPort(pipeline.channel());
    Credentials credentials = credentialsProvider.get(remoteAddress);
    pipeline.addLast(new AuthHandler(credentials));
  }
}
