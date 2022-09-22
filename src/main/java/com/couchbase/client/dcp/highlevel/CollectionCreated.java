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

package com.couchbase.client.dcp.highlevel;

import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.DatabaseChangeEvent;
import com.couchbase.client.dcp.message.DcpSystemEvent;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

import java.util.OptionalLong;

public class CollectionCreated extends DcpSystemEvent implements DcpSystemEvent.CollectionsManifestEvent, DatabaseChangeEvent {
  private final long newManifestId;
  private final long scopeId;
  private final long collectionId;
  private final String collectionName;
  private final OptionalLong maxTtl;

  public CollectionCreated(int vbucket, long seqno, int version, ByteBuf buffer) {
    super(Type.COLLECTION_CREATED, vbucket, seqno, version);

    collectionName = MessageUtil.getKeyAsString(buffer);
    ByteBuf value = MessageUtil.getContent(buffer);

    newManifestId = value.readLong();
    scopeId = value.readUnsignedInt();
    collectionId = value.readUnsignedInt();

    // absent in version 0
    maxTtl = value.isReadable() ? OptionalLong.of(value.readUnsignedInt()) : OptionalLong.empty();
  }

  @Override
  public long getManifestId() {
    return newManifestId;
  }

  public long getScopeId() {
    return scopeId;
  }

  public long getCollectionId() {
    return collectionId;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public OptionalLong getMaxTtl() {
    return maxTtl;
  }

  @Override
  public CollectionsManifest apply(CollectionsManifest currentManifest) {
    final Long maxTtl = this.maxTtl.isPresent() ? this.maxTtl.getAsLong() : null;
    return currentManifest.withCollection(newManifestId, scopeId, collectionId, collectionName, maxTtl);
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onCollectionCreated(this);
  }

  @Override
  public String toString() {
    return "CollectionCreated{" +
        "newManifestId=" + newManifestId +
        ", scopeId=" + scopeId +
        ", collectionId=" + collectionId +
        ", collectionName='" + collectionName + '\'' +
        ", maxTtl=" + maxTtl +
        ", vbucket=" + getVbucket() +
        ", seqno=" + getSeqno() +
        ", version=" + getVersion() +
        '}';
  }
}
