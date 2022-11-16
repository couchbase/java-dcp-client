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

import com.github.therapi.core.annotation.Remotable;

import java.util.List;

@Remotable("collection")
public interface CollectionService {

  /**
   * Blocks until scopes exist on the server, or fails
   *
   * @param scopes List of scope names to create
   * @param bucket Name of bucket to create scopes on
   */
  void createScopes(List<String> scopes, String bucket);

  /**
   * Blocks until collections exist on the server, or fails
   *
   * @param collections List of collection names to create
   * @param scope The scope to create the collections on
   * @param bucket Name of the bucket to create the collections on
   */
  void createCollections(List<String> collections, String scope, String bucket);

  /**
   * Deletes scopes from bucket if they exist, otherwise does nothing
   *
   * @param scopes List of scope names to delete
   * @param bucket Name of bucket to delete scopes off
   */
  void deleteScopes(List<String> scopes, String bucket);

  /**
   * Delete collections off scope if they exist, otherwise does nothing
   *
   * @param collections List of collection names to delete
   * @param scope The scope to delete the collections off
   * @param bucket Name of the bucket to delete the collections off
   */
  void deleteCollections(List<String> collections, String scope, String bucket);

  /**
   * @param bucket Name of bucket to create scopes on
   * @param scopeIdPrefix Prefix for each scope id
   * @param scopes Number of scopes to create
   * @return List of scope names that were created
   */
  List<String> createScopesWithPrefix(String bucket, String scopeIdPrefix, int scopes);

  /**
   * @param bucket Name of bucket to use
   * @param scope Name of scope to create collections on
   * @param collectionIdPrefix Prefix for each collection id
   * @param collections Number of collections to create
   * @return List of collection names that were created
   */
  List<String> createCollectionsWithPrefix(String bucket, String scope, String collectionIdPrefix, int collections);

}

