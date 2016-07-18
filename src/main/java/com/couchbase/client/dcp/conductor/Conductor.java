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
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.deps.io.netty.util.internal.ConcurrentSet;
import rx.Completable;
import rx.functions.Action1;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

    private final ConfigProvider configProvider;
    private final Set<DcpChannel> channels;
    private volatile long configRev = -1;
    private final ClientEnvironment env;
    private final AtomicReference<CouchbaseBucketConfig> currentConfig;


    public Conductor(final ClientEnvironment env, ConfigProvider cp) {
        this.env = env;
        this.currentConfig = new AtomicReference<CouchbaseBucketConfig>();

        configProvider = cp == null ? new HttpStreamingConfigProvider(env) : cp;
        configProvider.configs().forEach(new Action1<CouchbaseBucketConfig>() {
            @Override
            public void call(CouchbaseBucketConfig config) {
                if (config.rev() > configRev) {
                    configRev = config.rev();
                    LOGGER.debug("Applying new configuration, rev is now {}.", configRev);
                    currentConfig.set(config);
                    reconfigure(config);
                } else {
                 LOGGER.debug("Ignoring config, since rev has not changed.");
                }
            }
        });
        channels = new ConcurrentSet<DcpChannel>();
    }

    public Completable connect() {
        Completable atLeastOneConfig = configProvider.configs().first().toCompletable();
        return configProvider.start().concatWith(atLeastOneConfig);
    }

    public Completable stop() {
        return configProvider.stop();
    }

    /**
     * Returns the total number of partitions.
     */
    public int numberOfPartitions() {
        CouchbaseBucketConfig config = currentConfig.get();
        return config.numberOfPartitions();
    }


    public Completable startStreamForPartition(short partition) {
        DcpChannel channel = masterChannelByPartition(partition);
        return channel.openStream(partition, 0, 0, 0, 0, 0);
    }

    /**
     * Returns the dcp channel responsible for a given vbucket id according to the current
     * configuration.
     *
     * Note that this doesn't mean that the partition is enabled there, it just checks the current
     * mapping.
     */
    private DcpChannel masterChannelByPartition(short partition) {
        CouchbaseBucketConfig config = currentConfig.get();
        int index = config.nodeIndexForMaster(partition, false);
        NodeInfo node = config.nodeAtIndex(index);
        for (DcpChannel ch : channels) {
            if (ch.hostname().equals(node.hostname())) {
                return ch;
            }
        }
        throw new IllegalStateException("No DcpChannel found for partition " + partition);
    }

    private void reconfigure(CouchbaseBucketConfig config) {
        List<InetAddress> toAdd = new ArrayList<InetAddress>();
        List<DcpChannel> toRemove = new ArrayList<DcpChannel>();

        for (NodeInfo node : config.nodes()) {
            InetAddress hostname = node.hostname();
            if (!(node.services().containsKey(ServiceType.BINARY)
                || node.sslServices().containsKey(ServiceType.BINARY))) {
                continue; // we only care about kv nodes
            }
            if (!channels.contains(hostname)) {
                toAdd.add(hostname);
                LOGGER.debug("Planning to add {}", hostname);
            }
        }

        for (DcpChannel chan : channels) {
            boolean found = false;
            for (NodeInfo node : config.nodes()) {
                InetAddress hostname = node.hostname();
                if (hostname.equals(chan)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.debug("Planning to remove {}", chan);
                toRemove.add(chan);
            }
        }

        for (InetAddress add : toAdd) {
            add(add);
        }

        for (DcpChannel remove : toRemove) {
            remove(remove);
        }
    }

    private void add(final InetAddress node) {
        if (channels.contains(node)) {
            return;
        }

        DcpChannel channel = new DcpChannel(node, env);
        channels.add(channel);
        channel.connect();
    }

    private void remove(DcpChannel node) {
       if(channels.remove(node)) {
           node.disconnect();
       }
    }
}
