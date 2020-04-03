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

package com.couchbase.client.dcp.message;

import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DcpSystemEvent {
  private static final Logger log = LoggerFactory.getLogger(DcpSystemEvent.class);

  public interface CollectionsManifestEvent {
    CollectionsManifest apply(CollectionsManifest currentManifest);

    long getManifestId();
  }

  public static class CollectionCreated extends DcpSystemEvent implements CollectionsManifestEvent {
    final long newManifestId;
    final long scopeId;
    final long collectionId;
    final String collectionName;
    final Long maxTtl;

    public CollectionCreated(int vbucket, long seqno, int version, ByteBuf buffer) {
      super(Type.COLLECTION_CREATED, vbucket, seqno, version);

      collectionName = MessageUtil.getKeyAsString(buffer);
      ByteBuf value = MessageUtil.getContent(buffer);

      newManifestId = value.readLong();
      scopeId = value.readUnsignedInt();
      collectionId = value.readUnsignedInt();

      // absent in version 0
      maxTtl = value.isReadable() ? value.readUnsignedInt() : null;
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
      return currentManifest.withCollection(newManifestId, scopeId, collectionId, collectionName, maxTtl);
    }

    @Override
    public long getManifestId() {
      return newManifestId;
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

  public static class CollectionDropped extends DcpSystemEvent implements CollectionsManifestEvent {
    final long newManifestId;
    final long collectionId;
    final long scopeId; // not useful, ignored

    public CollectionDropped(int vbucket, long seqno, int version, ByteBuf buffer) {
      super(Type.COLLECTION_DROPPED, vbucket, seqno, version);

      ByteBuf value = MessageUtil.getContent(buffer);

      newManifestId = value.readLong();
      scopeId = value.readUnsignedInt();
      collectionId = value.readUnsignedInt();
    }

    @Override
    public String toString() {
      return "CollectionDropped{" +
          "newManifestId=" + newManifestId +
          ", collectionId=" + collectionId +
          ", scopeId=" + scopeId +
          ", vbucket=" + getVbucket() +
          ", seqno=" + getSeqno() +
          ", version=" + getVersion() +
          '}';
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
      return currentManifest.withoutCollection(newManifestId, collectionId);
    }

    @Override
    public long getManifestId() {
      return newManifestId;
    }
  }

  public static class CollectionFlushed extends DcpSystemEvent implements CollectionsManifestEvent {
    final long newManifestId;
    final long collectionId;
    final long scopeId; // not useful, ignored

    public CollectionFlushed(int vbucket, long seqno, int version, ByteBuf buffer) {
      super(Type.COLLECTION_FLUSHED, vbucket, seqno, version);

      ByteBuf value = MessageUtil.getContent(buffer);

      newManifestId = value.readLong();
      scopeId = value.readUnsignedInt();
      collectionId = value.readUnsignedInt();
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
      return currentManifest.withManifestId(newManifestId);
    }

    @Override
    public long getManifestId() {
      return newManifestId;
    }

    @Override
    public String toString() {
      return "CollectionFlushed{" +
          "newManifestId=" + newManifestId +
          ", collectionId=" + collectionId +
          ", scopeId=" + scopeId +
          ", vbucket=" + getVbucket() +
          ", seqno=" + getSeqno() +
          ", version=" + getVersion() +
          '}';
    }
  }

  public static class ScopeCreated extends DcpSystemEvent implements CollectionsManifestEvent {
    final long newManifestId;
    final long scopeId;
    final String scopeName;

    public ScopeCreated(int vbucket, long seqno, int version, ByteBuf buffer) {
      super(Type.SCOPE_CREATED, vbucket, seqno, version);

      scopeName = MessageUtil.getKeyAsString(buffer);
      ByteBuf value = MessageUtil.getContent(buffer);

      newManifestId = value.readLong();
      scopeId = value.readUnsignedInt();
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
      return currentManifest.withScope(newManifestId, scopeId, scopeName);
    }

    @Override
    public long getManifestId() {
      return newManifestId;
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

  public static class ScopeDropped extends DcpSystemEvent implements CollectionsManifestEvent {
    final long newManifestId;
    final long scopeId;

    public ScopeDropped(int vbucket, long seqno, int version, ByteBuf buffer) {
      super(Type.SCOPE_DROPPED, vbucket, seqno, version);

      ByteBuf value = MessageUtil.getContent(buffer);

      newManifestId = value.readLong();
      scopeId = value.readUnsignedInt();
    }

    @Override
    public CollectionsManifest apply(CollectionsManifest currentManifest) {
      return currentManifest.withoutScope(newManifestId, scopeId);
    }

    @Override
    public long getManifestId() {
      return newManifestId;
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

  public enum Type {
    COLLECTION_CREATED(0),
    COLLECTION_DROPPED(1),
    COLLECTION_FLUSHED(2),
    SCOPE_CREATED(3),
    SCOPE_DROPPED(4);

    private final int code;

    Type(int code) {
      this.code = code;
    }

    public int code() {
      return code;
    }
  }

  private final Type type;
  private final int vbucket;
  private final long seqno;
  private final int version;

  private DcpSystemEvent(Type type, int vbucket, long seqno, int version) {
    this.vbucket = vbucket;
    this.type = type;
    this.seqno = seqno;
    this.version = version;
  }

  public Type getType() {
    return type;
  }

  public int getVbucket() {
    return vbucket;
  }

  public long getSeqno() {
    return seqno;
  }

  public int getVersion() {
    return version;
  }

  public static DcpSystemEvent parse(final ByteBuf buffer) {
    final int vbucket = MessageUtil.getVbucket(buffer);
    final ByteBuf extras = MessageUtil.getExtras(buffer);
    final long seqno = extras.readLong();
    final int typeCode = extras.readInt();
    final int version = extras.readUnsignedByte();

    switch (typeCode) {
      case 0:
        return new CollectionCreated(vbucket, seqno, version, buffer);
      case 1:
        return new CollectionDropped(vbucket, seqno, version, buffer);
      case 2:
        return new CollectionFlushed(vbucket, seqno, version, buffer);
      case 3:
        return new ScopeCreated(vbucket, seqno, version, buffer);
      case 4:
        return new ScopeDropped(vbucket, seqno, version, buffer);
      default:
        log.warn("Ignoring unrecognized DCP system event type {}", typeCode);
        return null;
    }

  }

}
