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

public class ScopeCreated extends DcpSystemEvent implements DcpSystemEvent.CollectionsManifestEvent, DatabaseChangeEvent {
  private final long newManifestId;
  private final long scopeId;
  private final String scopeName;

  public ScopeCreated(int vbucket, long seqno, int version, ByteBuf buffer) {
    super(Type.SCOPE_CREATED, vbucket, seqno, version);

    scopeName = MessageUtil.getKeyAsString(buffer);
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

  public String getScopeName() {
    return scopeName;
  }

  @Override
  public CollectionsManifest apply(CollectionsManifest currentManifest) {
    return currentManifest.withScope(newManifestId, scopeId, scopeName);
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onScopeCreated(this);
  }

  @Override
  public String toString() {
    return "ScopeCreated{" +
        "newManifestId=" + newManifestId +
        ", scopeId=" + scopeId +
        ", scopeName='" + scopeName + '\'' +
        ", vbucket=" + getVbucket() +
        ", seqno=" + getSeqno() +
        ", version=" + getVersion() +
        '}';
  }
}
