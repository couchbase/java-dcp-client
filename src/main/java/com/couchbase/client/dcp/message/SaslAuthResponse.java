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
package com.couchbase.client.dcp.message;

import io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_AUTH_OPCODE;

public enum SaslAuthResponse {
  ;

  /**
   * If the given buffer is a {@link SaslAuthResponse} message.
   */
  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_RES && buffer.getByte(1) == SASL_AUTH_OPCODE;
  }

  /**
   * Returns the server challenge.
   */
  public static ByteBuf challenge(final ByteBuf buffer) {
    return MessageUtil.getContent(buffer);
  }

}
