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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.DatabaseChangeEvent;
import com.couchbase.client.dcp.message.DcpSystemEvent;
import com.couchbase.client.dcp.message.MessageUtil;

public class ScopeDropped extends DcpSystemEvent implements DcpSystemEvent.CollectionsManifestEvent, DatabaseChangeEvent {
  private final long newManifestId;
  private final long scopeId;

  public ScopeDropped(int vbucket, long seqno, int version, ByteBuf buffer) {
    super(Type.SCOPE_DROPPED, vbucket, seqno, version);

    ByteBuf value = MessageUtil.getContent(buffer);

    newManifestId = value.readLong();
    scopeId = value.readUnsignedInt();
  }

  @Override
  public long getManifestId() {
    return newManifestId;
  }

  public long getScopeId() {
    return scopeId;
  }

  @Override
  public CollectionsManifest apply(CollectionsManifest currentManifest) {
    return currentManifest.withoutScope(newManifestId, scopeId);
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onScopeDropped(this);
  }

  @Override
  public String toString() {
    return "ScopeDropped{" +
        "newManifestId=" + newManifestId +
        ", scopeId=" + scopeId +
        ", vbucket=" + getVbucket() +
        ", seqno=" + getSeqno() +
        ", version=" + getVersion() +
        '}';
  }
}
