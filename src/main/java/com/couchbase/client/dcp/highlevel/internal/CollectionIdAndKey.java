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

import static java.util.Objects.requireNonNull;

public class CollectionIdAndKey {
  private final long collectionId;
  private final String key;

  public CollectionIdAndKey(long collectionId, String key) {
    this.collectionId = collectionId;
    this.key = requireNonNull(key);
  }

  public static CollectionIdAndKey forDefaultCollection(String key) {
    return new CollectionIdAndKey(0, key);
  }

  public long collectionId() {
    return collectionId;
  }

  public String key() {
    return key;
  }

  @Override
  public String toString() {
    return collectionId + "/" + key;
  }
}
