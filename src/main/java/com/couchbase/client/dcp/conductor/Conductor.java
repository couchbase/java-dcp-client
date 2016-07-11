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
import com.couchbase.client.dcp.DefaultConnectionNameGenerator;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.deps.io.netty.channel.oio.OioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.channel.socket.oio.OioSocketChannel;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.functions.Action1;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

    private final ConfigProvider configProvider;
    private final Map<InetAddress, Channel> dataChannels;
    private volatile long configRev = -1;
    private final ClientEnvironment env;

    public Conductor(final ClientEnvironment env) {
        this.env = env;

        configProvider = new ConfigProvider(env);
        configProvider.configs().forEach(new Action1<CouchbaseBucketConfig>() {
            @Override
            public void call(CouchbaseBucketConfig config) {
                if (config.rev() > configRev) {
                    configRev = config.rev();
                    LOGGER.debug("Applying new configuration, rev is now {}.", configRev);
                    reconfigure(config);
                } else {
                 LOGGER.debug("Ignoring config, since rev has not changed.");
                }
            }
        });
        dataChannels = new ConcurrentHashMap<InetAddress, Channel>();
    }

    public Completable connect() {
        return configProvider.start();
    }

    public void stop() {
        configProvider.stop();
    }

    private void reconfigure(CouchbaseBucketConfig config) {
        List<InetAddress> toAdd = new ArrayList<InetAddress>();
        List<InetAddress> toRemove = new ArrayList<InetAddress>();

        for (NodeInfo node : config.nodes()) {
            InetAddress hostname = node.hostname();
            if (!(node.services().containsKey(ServiceType.BINARY)
                || node.sslServices().containsKey(ServiceType.BINARY))) {
                continue; // we only care about kv nodes
            }
            if (!dataChannels.containsKey(hostname)) {
                toAdd.add(hostname);
                LOGGER.debug("Planning to add {}", hostname);
            }
        }

        for (InetAddress chan : dataChannels.keySet()) {
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

        for (InetAddress remove : toRemove) {
            remove(remove);
        }
    }

    private void add(final InetAddress node) {
        if (dataChannels.containsKey(node)) {
            return;
        }


        final Bootstrap bootstrap = new Bootstrap()
            .remoteAddress(node, 11210)
            .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
            // FIXME
            .handler(new DcpPipeline(DefaultConnectionNameGenerator.INSTANCE, env.bucket(), env.password(), new DcpControl()))
            .group(env.eventLoopGroup());

        bootstrap.connect().addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    dataChannels.put(node, future.channel());
                } else {
                    LOGGER.warn("IMPLEMENT ME!!! (retry on failure until removed)");
                }
            }
        });
    }

    private void remove(InetAddress node) {
        Channel channel = dataChannels.remove(node);
        if (channel != null) {
            channel.close().addListener(new GenericFutureListener<ChannelFuture>() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        LOGGER.debug("Error during channel close.", future.cause());
                    }
                }
            });
        }
    }
}
