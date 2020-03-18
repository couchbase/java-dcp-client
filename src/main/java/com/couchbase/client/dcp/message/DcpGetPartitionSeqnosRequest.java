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
import io.netty.buffer.Unpooled;

import static com.couchbase.client.dcp.message.MessageUtil.GET_SEQNOS_OPCODE;

public enum DcpGetPartitionSeqnosRequest {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == GET_SEQNOS_OPCODE;
  }

  public static void init(final ByteBuf buffer) {
    MessageUtil.initRequest(GET_SEQNOS_OPCODE, buffer);
  }

  public static void opaque(final ByteBuf buffer, int opaque) {
    MessageUtil.setOpaque(opaque, buffer);
  }

  public static void vbucketState(final ByteBuf buffer, VbucketState vbucketState) {

    switch (vbucketState) {
      case ANY:
        break;
      case ACTIVE:
      case REPLICA:
      case PENDING:
      case DEAD:
        ByteBuf extras = Unpooled.buffer(4);
        MessageUtil.setExtras(extras.writeInt(vbucketState.value()), buffer);
        extras.release();
    }
  }

}
