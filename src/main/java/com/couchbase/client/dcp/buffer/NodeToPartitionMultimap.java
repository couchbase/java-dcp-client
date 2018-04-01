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

package com.couchbase.client.dcp.buffer;

import com.couchbase.client.core.config.CouchbaseBucketConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

/**
 * A map from node index to the partitions hosted on the node (including active and replica).
 * <p>
 * Immutable.
 */
class NodeToPartitionMultimap {

    private final Map<Integer, List<PartitionInstance>> nodeIndexToHostedPartitions =
            new HashMap<Integer, List<PartitionInstance>>();

    NodeToPartitionMultimap(final CouchbaseBucketConfig bucketConfig) {
        for (short partition = 0; partition < bucketConfig.numberOfPartitions(); partition++) {
            put(bucketConfig.nodeIndexForMaster(partition, false), new PartitionInstance(partition, 0));
            for (int r = 0; r < bucketConfig.numberOfReplicas(); r++) {
                put(bucketConfig.nodeIndexForReplica(partition, r, false), new PartitionInstance(partition, r + 1));
            }
        }

        freezeValues(nodeIndexToHostedPartitions);
    }

    private static <K, V> void freezeValues(final Map<K, List<V>> map) {
        for (Map.Entry<K, List<V>> entry : map.entrySet()) {
            entry.setValue(unmodifiableList(entry.getValue()));
        }
    }

    private void put(final int nodeIndex, final PartitionInstance partition) {
        List<PartitionInstance> hostedPartitions = nodeIndexToHostedPartitions.get(nodeIndex);
        if (hostedPartitions == null) {
            hostedPartitions = new ArrayList<PartitionInstance>(4);
            nodeIndexToHostedPartitions.put(nodeIndex, hostedPartitions);
        }
        hostedPartitions.add(partition);
    }

    List<PartitionInstance> get(final int nodeIndex) {
        List<PartitionInstance> hostedPartitions = nodeIndexToHostedPartitions.get(nodeIndex);
        if (hostedPartitions == null) {
            throw new IllegalArgumentException("Node " + nodeIndex + " does not exist");
        }
        return hostedPartitions;
    }

    /**
     * Returns the partition instances whose node indexes are < 0.
     */
    List<PartitionInstance> getAbsent() {
        List<PartitionInstance> absentPartitions = new ArrayList<PartitionInstance>();
        for (Map.Entry<Integer, List<PartitionInstance>> e : nodeIndexToHostedPartitions.entrySet()) {
            if (e.getKey() < 0) {
                absentPartitions.addAll(e.getValue());
            }
        }
        return absentPartitions;
    }

    @Override
    public String toString() {
        return nodeIndexToHostedPartitions.toString();
    }
}
