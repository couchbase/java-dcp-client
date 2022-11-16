/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp.highlevel.internal;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.core.utils.UnsignedLEB128;
import com.couchbase.client.dcp.message.MessageUtil;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Knows how to parse the collection ID out of a message's key field.
 */
public interface KeyExtractor {
  // When collections are enabled, the key is prefixed by a LEB-128 encoded collection ID.
  KeyExtractor COLLECTIONS = event -> {
    ByteBuf keyBuf = MessageUtil.getKey(event);
    return new CollectionIdAndKey(UnsignedLEB128.read(keyBuf), keyBuf.toString(UTF_8));
  };

  // When collections are disabled, every key is implicitly in the default collection which has ID zero.
  KeyExtractor NO_COLLECTIONS = event -> CollectionIdAndKey.forDefaultCollection(
      MessageUtil.getKeyAsString(event));

  CollectionIdAndKey getCollectionIdAndKey(ByteBuf event);
}
