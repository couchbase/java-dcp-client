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

public class BucketConfigHelper {
    private final boolean sslEnabled;
    private final NodeToPartitionMultimap map;
    private final List<NodeInfo> dataNodes;

    public BucketConfigHelper(final CouchbaseBucketConfig config, final boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        this.map = new NodeToPartitionMultimap(config);

        final List<NodeInfo> tempDataNodes = new ArrayList<NodeInfo>();
        for (NodeInfo node : config.nodes()) {
            if (hasBinaryService(node)) {
                tempDataNodes.add(node);
            }
        }
        this.dataNodes = unmodifiableList(tempDataNodes);
    }

    public List<PartitionInstance> getHostedPartitions(final InetSocketAddress nodeAddress) throws NoSuchElementException {
        int nodeIndex = getNodeIndex(nodeAddress);
        return map.get(nodeIndex);
    }

    /**
     * Returns an unmodifiable list containing only those nodes that are running the Data Service
     * (also known as Key/Value Service or Binary Service)
     */
    public List<NodeInfo> getDataNodes() {
        return dataNodes;
    }

    public int getNodeIndex(final InetSocketAddress nodeAddress) throws NoSuchElementException {
        int nodeIndex = 0;
        for (NodeInfo node : getDataNodes()) {
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
        return new InetSocketAddress(node.hostname().nameOrAddress(), port);
    }

    private Map<ServiceType, Integer> getServicePortMap(final NodeInfo node) {
        return sslEnabled ? node.sslServices() : node.services();
    }

    private boolean hasBinaryService(final NodeInfo node) {
        return getServicePortMap(node).containsKey(ServiceType.BINARY);
    }
}
