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
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.RedactableArgument;
import com.couchbase.client.core.service.ServiceType;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class BucketConfigHelper {
    private final boolean sslEnabled;
    private final CouchbaseBucketConfig config;
    private final NodeToPartitionMultimap map;
    private final List<NodeInfo> dataNodesWithAnyPartitions;
    private final List<NodeInfo> dataNodesWithActivePartitions;

    public BucketConfigHelper(final CouchbaseBucketConfig config, final boolean sslEnabled) {
        this.config = requireNonNull(config);
        this.sslEnabled = sslEnabled;
        this.map = new NodeToPartitionMultimap(config);

        final List<NodeInfo> active = new ArrayList<>();
        final List<NodeInfo> activeOrReplica = new ArrayList<>();

        for (int i = 0, len = config.nodes().size(); i < len; i++) {
            final NodeInfo node = config.nodes().get(i);
            final boolean hasAnyPartitions = !map.get(i).isEmpty();

            if (hasAnyPartitions) {
                if (!hasBinaryService(node)) {
                    throw new IllegalArgumentException("Only nodes running the KV service can host bucket partitions.");
                }

                activeOrReplica.add(node);

                final boolean nodeHasActivePartitions =
                        map.get(i).stream().anyMatch(partitionInstance -> partitionInstance.slot() == 0);

                if (nodeHasActivePartitions) {
                    active.add(node);
                }
            }
        }

        this.dataNodesWithAnyPartitions = unmodifiableList(activeOrReplica);
        this.dataNodesWithActivePartitions = unmodifiableList(active);
    }

    public List<PartitionInstance> getHostedPartitions(final InetSocketAddress nodeAddress) throws NoSuchElementException {
        int nodeIndex = getNodeIndex(nodeAddress);
        return map.get(nodeIndex);
    }

    /**
     * Returns an unmodifiable list containing only those nodes that are running the Data Service
     * (also known as Key/Value Service or Binary Service) and are hosting at least one active or replica partition.
     *
     * @param requireActivePartition if true, only nodes hosting at least one active partition will be returned.
     * Otherwise, nodes hosting at least partition of any kind (active or replica) will be returned.
     */
    public List<NodeInfo> getDataNodes(boolean requireActivePartition) {
        return requireActivePartition ? dataNodesWithActivePartitions : dataNodesWithAnyPartitions;
    }

    public int getNodeIndex(final InetSocketAddress nodeAddress) throws NoSuchElementException {
        int nodeIndex = 0;
        for (NodeInfo node : config.nodes()) {
            if (nodeAddress.equals(getAddress(node))) {
                return nodeIndex;
            }
            nodeIndex++;
        }
        throw new NoSuchElementException("Failed to locate " + RedactableArgument.system(nodeAddress) + " in bucket config.");
    }

    public List<PartitionInstance> getAbsentPartitionInstances() {
        return map.getAbsent();
    }

    public InetSocketAddress getAddress(final NodeInfo node) {
        int port = getServicePortMap(node).get(ServiceType.BINARY);
        return new InetSocketAddress(node.rawHostname(), port);
    }

    private Map<ServiceType, Integer> getServicePortMap(final NodeInfo node) {
        return sslEnabled ? node.sslServices() : node.services();
    }

    private boolean hasBinaryService(final NodeInfo node) {
        return getServicePortMap(node).containsKey(ServiceType.BINARY);
    }
}
