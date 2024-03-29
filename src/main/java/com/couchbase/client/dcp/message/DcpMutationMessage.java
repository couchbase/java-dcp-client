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

import java.nio.charset.Charset;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_MUTATION_OPCODE;
import static java.nio.charset.StandardCharsets.UTF_8;

public enum DcpMutationMessage {
  ;

  public static boolean is(final ByteBuf buffer) {
    return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_MUTATION_OPCODE;
  }

  /**
   * @deprecated This method parses the key incorrectly if collections are enabled.
   * Please use {@link MessageUtil#getCollectionIdAndKey(ByteBuf, boolean)} instead.
   */
  @Deprecated
  public static ByteBuf key(final ByteBuf buffer) {
    return MessageUtil.getKey(buffer);
  }

  /**
   * @deprecated This method parses the key incorrectly if collections are enabled.
   * Please use {@link MessageUtil#getCollectionIdAndKey(ByteBuf, boolean)} instead.
   */
  @Deprecated
  public static String keyString(final ByteBuf buffer, Charset charset) {
    return key(buffer).toString(charset);
  }

  /**
   * @deprecated This method parses the key incorrectly if collections are enabled.
   * Please use {@link MessageUtil#getCollectionIdAndKey(ByteBuf, boolean)} instead.
   */
  @Deprecated
  public static String keyString(final ByteBuf buffer) {
    return keyString(buffer, UTF_8);
  }

  public static ByteBuf content(final ByteBuf buffer) {
    return MessageUtil.getContent(buffer);
  }

  public static byte[] contentBytes(final ByteBuf buffer) {
    return MessageUtil.getContentAsByteArray(buffer);
  }

  public static long cas(final ByteBuf buffer) {
    return MessageUtil.getCas(buffer);
  }

  public static int partition(final ByteBuf buffer) {
    return MessageUtil.getVbucket(buffer);
  }

  public static long bySeqno(final ByteBuf buffer) {
    return buffer.getLong(MessageUtil.HEADER_SIZE);
  }

  public static long revisionSeqno(final ByteBuf buffer) {
    return buffer.getLong(MessageUtil.HEADER_SIZE + 8);
  }

  public static int flags(final ByteBuf buffer) {
    return buffer.getInt(MessageUtil.HEADER_SIZE + 16);
  }

  public static int expiry(final ByteBuf buffer) {
    return buffer.getInt(MessageUtil.HEADER_SIZE + 20);
  }

  public static int lockTime(final ByteBuf buffer) {
    return buffer.getInt(MessageUtil.HEADER_SIZE + 24);
  }

  public static String toString(final ByteBuf buffer) {
    return "MutationMessage [key: \"" + keyString(buffer)
        + "\", vbid: " + partition(buffer)
        + ", cas: " + cas(buffer)
        + ", bySeqno: " + bySeqno(buffer)
        + ", revSeqno: " + revisionSeqno(buffer)
        + ", flags: " + flags(buffer)
        + ", expiry: " + expiry(buffer)
        + ", lockTime: " + lockTime(buffer)
        + ", clength: " + content(buffer).readableBytes() + "]";
  }
}
