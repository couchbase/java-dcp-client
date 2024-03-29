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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.SASL_LIST_MECHS_OPCODE;

public enum SaslListMechsRequest {
  ;

  /**
   * If the given buffer is a {@link SaslListMechsRequest} message.
   */
  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == SASL_LIST_MECHS_OPCODE;
  }

  /**
   * Initialize the buffer with all the values needed.
   */
  public static void init(final ByteBuf buffer) {
    MessageUtil.initRequest(SASL_LIST_MECHS_OPCODE, buffer);
  }

}
