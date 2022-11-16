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

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.core.error.ScopeNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.couchbase.client.dcp.test.util.Poller.poll;

@Service
public class CollectionServiceImpl implements CollectionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionServiceImpl.class);

  private final ClusterSupplier clusterSupplier;
  private final StreamerService streamerService;

  @Autowired
  public CollectionServiceImpl(ClusterSupplier clusterSupplier, StreamerService streamerService) {
    this.clusterSupplier = clusterSupplier;
    this.streamerService = streamerService;
  }

  private Cluster cluster() {
    return clusterSupplier.get();
  }

  private CollectionManager collectionManager(String bucket) {
    return cluster().bucket(bucket).collections();
  }

  @Override
  public List<String> createScopesWithPrefix(String bucket, String scopeIdPrefix, int scopes) {
    List<String> scopeIds = generateIds(scopeIdPrefix, scopes);
    createScopes(scopeIds, bucket);
    LOGGER.info("{} Scopes created, with prefix {}", scopes, scopeIdPrefix);
    return scopeIds;
  }

  @Override
  public List<String> createCollectionsWithPrefix(String bucket, String scope, String collectionIdPrefix, int collections) {
    List<String> collectionIds = generateIds(collectionIdPrefix, collections);
    createCollections(collectionIds, scope, bucket);
    LOGGER.info("{} Collections created, with prefix {}, on scope {}", collections, collectionIdPrefix, scope);
    return collectionIds;
  }

  @Override
  public void createScopes(List<String> scopes, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    scopes.forEach(scopeId -> {
      collectionManager.createScope(scopeId);
      poll().withTimeout(10, TimeUnit.SECONDS).until(() -> scopeExists(scopeId, collectionManager));
    });
  }

  @Override
  public void createCollections(List<String> collections, String scope, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    collections.forEach(collectionID -> {
      CollectionSpec collectionSpec = CollectionSpec.create(collectionID, scope);
      collectionManager.createCollection(collectionSpec);
      poll().withTimeout(10, TimeUnit.SECONDS).until(() -> collectionExists(collectionSpec, collectionManager));
    });
  }

  @Override
  public void deleteScopes(List<String> scopes, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    scopes.forEach(scopeId -> collectionManager.dropScope(scopeId));
  }

  @Override
  public void deleteCollections(List<String> collections, String scope, String bucket) {
    CollectionManager collectionManager = collectionManager(bucket);
    collections.forEach(collectionID -> collectionManager.dropCollection(CollectionSpec.create(collectionID, scope)));
  }

  /**
   * Helper to check if scope exists
   *
   * @param scopeName Name of scope
   * @param cm CollectionManager on which the scope is supposed to be
   * @return True if the scope exists
   */
  private static boolean scopeExists(String scopeName, CollectionManager cm) {
    try {
      cm.getScope(scopeName);
      return true;
    } catch (ScopeNotFoundException e) {
      return false;
    }
  }

  /**
   * Helper to check if a collection exists
   *
   * @param spec Collection spec to check
   * @param cm CollectionManager where the collection is supposed to be
   * @return True if the collection exists
   */
  private static boolean collectionExists(CollectionSpec spec, CollectionManager cm) {
    try {
      ScopeSpec scope = cm.getScope(spec.scopeName());
      return scope.collections().contains(spec);
    } catch (ScopeNotFoundException e) {
      return false;
    }
  }

  private List<String> generateIds(String prefix, int numberToGenerate) {
    return IntStream.rangeClosed(0, numberToGenerate - 1).mapToObj(i -> prefix + i).collect(Collectors.toList());
  }
}
