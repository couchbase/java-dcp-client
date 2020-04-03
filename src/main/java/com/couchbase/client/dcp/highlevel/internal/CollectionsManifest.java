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

import com.couchbase.client.dcp.core.utils.DefaultObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;

import static com.couchbase.client.dcp.core.utils.CbCollections.mapOf;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * A collections manifest optimized for lookups by collection ID,
 * and for evolution via DCP system events.
 * <p>
 * Immutable.
 */
public class CollectionsManifest {

  /**
   * A manifest with just the default scope and default collection.
   */
  public static final CollectionsManifest DEFAULT = defaultManifest();

  public static class ScopeInfo {
    public static final ScopeInfo DEFAULT = new ScopeInfo(0, "_default");

    private final long id;
    private final String name;

    public ScopeInfo(long id, String name) {
      this.id = id;
      this.name = requireNonNull(name);
    }

    public long id() {
      return id;
    }

    public String name() {
      return name;
    }

    @Override
    public String toString() {
      return id + ":" + name;
    }
  }

  public static class CollectionInfo {
    private final ScopeInfo scope;
    private final long id;
    private final String name;
    private final OptionalLong maxTtl;

    public CollectionInfo(ScopeInfo scope, long id, String name, Long maxTtl) {
      this.id = id;
      this.name = requireNonNull(name);

      this.scope = requireNonNull(scope);
      this.maxTtl = maxTtl == null ? OptionalLong.empty() : OptionalLong.of(maxTtl);
    }

    public ScopeInfo scope() {
      return scope;
    }

    public long id() {
      return id;
    }

    public String name() {
      return name;
    }

    public OptionalLong maxTtl() {
      return maxTtl;
    }

    @Override
    public String toString() {
      return "CollectionInfo{" +
          "scope=" + scope +
          ", id=" + id +
          ", name='" + name + '\'' +
          ", maxTtl=" + maxTtl +
          '}';
    }
  }

  private final long id;
  private final Map<Long, ScopeInfo> scopesById;
  private final Map<Long, CollectionInfo> collectionsById;

  private static <K, V> Map<K, V> copyToUnmodifiableMap(Map<K, V> map) {
    return unmodifiableMap(new HashMap<>(map));
  }

  private CollectionsManifest(long manifestId, Map<Long, ScopeInfo> scopesById, Map<Long, CollectionInfo> collectionsById) {
    this.id = manifestId;
    this.scopesById = copyToUnmodifiableMap(scopesById);
    this.collectionsById = copyToUnmodifiableMap(collectionsById);
  }

  private static CollectionsManifest defaultManifest() {
    ScopeInfo defaultScope = ScopeInfo.DEFAULT;
    ScopeInfo defaultCollection = ScopeInfo.DEFAULT;
    return new CollectionsManifest(0,
        mapOf(defaultScope.id(), defaultScope),
        mapOf(defaultCollection.id(), new CollectionInfo(defaultScope, defaultCollection.id(), defaultCollection.name(), null)));
  }

  public CollectionsManifest withManifestId(long newManifestId) {
    return new CollectionsManifest(newManifestId, scopesById, collectionsById);
  }

  public CollectionsManifest withScope(long newManifestId, long newScopeId, String newScopeName) {
    Map<Long, ScopeInfo> newScopeMap = new HashMap<>(scopesById);
    newScopeMap.put(newScopeId, new ScopeInfo(newScopeId, newScopeName));
    return new CollectionsManifest(newManifestId, newScopeMap, collectionsById);
  }

  public CollectionsManifest withoutScope(long newManifestId, long doomedScopeId) {
    Map<Long, ScopeInfo> newScopeMap = new HashMap<>(scopesById);
    newScopeMap.remove(doomedScopeId);

    Map<Long, CollectionInfo> newCollectionMap =
        collectionsById.entrySet()
            .stream()
            .filter(e -> e.getValue().scope().id() != doomedScopeId)
            .collect(toMap(Entry::getKey, Entry::getValue));

    return new CollectionsManifest(newManifestId, newScopeMap, newCollectionMap);
  }

  public CollectionsManifest withCollection(long newManifestId, long scopeId, long collectionId, String collectionName, Long maxTtl) {
    final ScopeInfo scopeIdAndName = scopesById.get(scopeId);
    if (scopeIdAndName == null) {
      throw new IllegalStateException("Unrecognized scope ID: " + scopeId);
    }
    Map<Long, CollectionInfo> newCollectionMap = new HashMap<>(collectionsById);
    newCollectionMap.put(collectionId, new CollectionInfo(scopeIdAndName, collectionId, collectionName, maxTtl));

    return new CollectionsManifest(newManifestId, scopesById, newCollectionMap);
  }

  public CollectionsManifest withoutCollection(long newManifestId, long id) {
    Map<Long, CollectionInfo> newCollectionMap =
        collectionsById.entrySet()
            .stream()
            .filter(e -> e.getValue().id() != id)
            .collect(toMap(Entry::getKey, Entry::getValue));

    return new CollectionsManifest(newManifestId, scopesById, newCollectionMap);
  }

  public CollectionInfo getCollection(long id) {
    return collectionsById.get(id);
  }

  public CollectionInfo getCollection(String name) {
    return collectionsById.values().stream()
        .filter(c -> c.name().equals(name))
        .findFirst()
        .orElse(null);
  }

  public ScopeInfo getScope(String name) {
    return scopesById.values().stream()
        .filter(s -> s.name().equals(name))
        .findFirst()
        .orElse(null);
  }

  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return "CollectionsManifest{" +
        "id=" + id +
        ", scopesById=" + scopesById +
        ", collectionsById=" + collectionsById +
        '}';
  }


  public static CollectionsManifest fromJson(byte[] jsonBytes) throws IOException {
    ManifestJson manifestBinder = DefaultObjectMapper.readValue(jsonBytes, ManifestJson.class);
    return manifestBinder.build(); // crazy inefficient, with all the map copying.
  }

  private static long parseId(String id) {
    return Long.parseUnsignedLong(id, 16);
  }

  @SuppressWarnings("WeakerAccess")
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class ManifestJson {
    public String uid;
    public List<ScopeJson> scopes;

    private CollectionsManifest build() {
      long manifestId = parseId(uid);
      CollectionsManifest m = new CollectionsManifest(manifestId, emptyMap(), emptyMap());
      for (ScopeJson s : scopes) {
        m = s.build(m, manifestId);
      }
      return m;
    }
  }

  @SuppressWarnings("WeakerAccess")
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class ScopeJson {
    public String uid;
    public String name;
    public List<CollectionJson> collections;

    private CollectionsManifest build(CollectionsManifest m, long manifestId) {
      long scopeId = parseId(uid);
      m = m.withScope(manifestId, scopeId, name);
      for (CollectionJson c : collections) {
        m = c.build(m, manifestId, scopeId);
      }
      return m;
    }
  }

  @SuppressWarnings("WeakerAccess")
  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class CollectionJson {
    public String uid;
    public String name;
    public Long max_ttl;

    private CollectionsManifest build(CollectionsManifest m, long manifestId, long scopeId) {
      return m.withCollection(manifestId, scopeId, parseId(uid), name, max_ttl);
    }
  }
}
