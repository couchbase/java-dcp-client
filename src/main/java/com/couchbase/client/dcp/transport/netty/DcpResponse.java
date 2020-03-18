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

package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.message.ResponseStatus;
import io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.getResponseStatus;
import static java.util.Objects.requireNonNull;

public class DcpResponse {
  private final ByteBuf buffer;

  // cache the status so it can still be retrieved after the buffer is released
  private final ResponseStatus status;

  public DcpResponse(ByteBuf buffer) {
    this.buffer = requireNonNull(buffer);
    this.status = getResponseStatus(buffer);
  }

  public ByteBuf buffer() {
    return buffer;
  }

  public ResponseStatus status() {
    return status;
  }
}
