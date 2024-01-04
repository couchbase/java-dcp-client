/*
 * Copyright 2018 Couchbase, Inc.
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

import reactor.util.annotation.Nullable;

import java.util.Set;

public interface BucketService {
  /**
   * Returns the names of all buckets.
   */
  Set<String> list();

  /**
   * Creates a bucket if it doesn't already exist.
   *
   * @param bucket Name of the bucket to create.
   * @param password Password to associate with the bucket (pre-Spock only).
   * @param quotaMb Memory quota in megabytes for the bucket.
   * @param replicas Number of replicas, or 0 to disable replication.
   * @param enableFlush Whether the bucket should support being flushed (reset to empty).
   */
  void create(String bucket, int quotaMb, int replicas, boolean enableFlush);

  /**
   * Deletes the named bucket if it exists, otherwise does nothing.
   *
   * @param bucket Name of the bucket to delete.
   */
  void delete(String bucket);
}
