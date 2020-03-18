/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.dcp.core.config;

import com.couchbase.client.dcp.core.config.BucketConfig;
import com.couchbase.client.dcp.core.config.DefaultCouchbaseBucketConfig;
import com.couchbase.client.dcp.core.config.NodeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * A configuration representing the couchbase bucket.
 */
@JsonDeserialize(as = DefaultCouchbaseBucketConfig.class)
public interface CouchbaseBucketConfig extends BucketConfig {

  /**
   * Returns the node index for the given partition index and master.
   *
   * @param partition the index of the partition.
   * @param useFastForward if the fast forward config should be used.
   * @return the index of the node.
   */
  short nodeIndexForMaster(int partition, boolean useFastForward);

  /**
   * Returns the node index for the given partition index and the replica.
   *
   * @param partition the index of the partition.
   * @param replica the replica number.
   * @param useFastForward if the fast forward config should be used.
   * @return the index of the node.
   */
  short nodeIndexForReplica(int partition, int replica, boolean useFastForward);

  /**
   * Returns the total number of partitions.
   *
   * @return the number of partitions.
   */
  int numberOfPartitions();

  /**
   * Returns information for the node at the given index.
   *
   * @param nodeIndex the index of the node.
   * @return the information of the node at this index.
   */
  NodeInfo nodeAtIndex(int nodeIndex);

  /**
   * The number of configured replicas for this bucket.
   *
   * @return number of replicas.
   */
  int numberOfReplicas();

  /**
   * Checks if the given hostname has active primary partitions assigned to it.
   *
   * @param hostname the hostname of the node to check against.
   * @return true if it has, false otherwise.
   */
  boolean hasPrimaryPartitionsOnNode(String hostname);

  /**
   * If this couchbase bucket is ephemeral.
   *
   * @return true if it is, false otherwise.
   */
  boolean ephemeral();
}
