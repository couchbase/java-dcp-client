/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import java.util.EnumSet;
import java.util.Set;

import static com.couchbase.client.dcp.message.HelloFeature.CLUSTERMAP_CHANGE_NOTIFICATION;
import static com.couchbase.client.dcp.message.HelloFeature.DUPLEX;
import static com.couchbase.client.dcp.message.HelloFeature.SELECT_BUCKET;
import static com.couchbase.client.dcp.message.HelloFeature.XERROR;
import static java.util.Collections.unmodifiableSet;

public enum HelloRequest {
  ;

  private static final Set<HelloFeature> standardFeatures = unmodifiableSet(EnumSet.of(
      XERROR, SELECT_BUCKET, DUPLEX, CLUSTERMAP_CHANGE_NOTIFICATION));

  public static void init(ByteBuf buffer, String connectionName, Set<HelloFeature> extraFeatures) {
    MessageUtil.initRequest(MessageUtil.HELLO_OPCODE, buffer);
    MessageUtil.setKey(connectionName, buffer);

    Set<HelloFeature> advertisedFeatures = EnumSet.copyOf(standardFeatures);
    advertisedFeatures.addAll(extraFeatures);

    ByteBuf features = Unpooled.buffer(advertisedFeatures.size() * 2);
    try {
      for (HelloFeature feature : advertisedFeatures) {
        features.writeShort(feature.code());
      }

      MessageUtil.setContent(features, buffer);

    } finally {
      features.release();
    }
  }

  public static Set<HelloFeature> parseResponse(ByteBuf msg) {
    Set<HelloFeature> features = EnumSet.noneOf(HelloFeature.class);
    ByteBuf content = MessageUtil.getContent(msg);
    while (content.isReadable()) {
      // Don't need to worry about encountering an unrecognized value,
      // since the response features are always a subset of the request features.
      features.add(HelloFeature.forCode(content.readShort()));
    }
    return features;
  }
}
