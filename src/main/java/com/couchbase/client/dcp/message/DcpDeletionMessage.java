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
import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_DELETION_OPCODE;

public enum DcpDeletionMessage {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_DELETION_OPCODE;
  }

  public static ByteBuf key(final ByteBuf buffer) {
    return MessageUtil.getKey(buffer);
  }

  public static String keyString(final ByteBuf buffer, Charset charset) {
    return key(buffer).toString(charset);
  }

  public static String keyString(final ByteBuf buffer) {
    return keyString(buffer, CharsetUtil.UTF_8);
  }


  public static long cas(final ByteBuf buffer) {
    return MessageUtil.getCas(buffer);
  }

  public static short partition(final ByteBuf buffer) {
    return MessageUtil.getVbucket(buffer);
  }

  public static long bySeqno(final ByteBuf buffer) {
    return buffer.getLong(MessageUtil.HEADER_SIZE);
  }

  public static long revisionSeqno(final ByteBuf buffer) {
    return buffer.getLong(MessageUtil.HEADER_SIZE + 8);
  }

  public static String toString(final ByteBuf buffer) {
    return "DeletionMessage [key: \"" + keyString(buffer)
        + "\", vbid: " + partition(buffer)
        + ", cas: " + cas(buffer)
        + ", bySeqno: " + bySeqno(buffer)
        + ", revSeqno: " + revisionSeqno(buffer) + "]";
  }

}
