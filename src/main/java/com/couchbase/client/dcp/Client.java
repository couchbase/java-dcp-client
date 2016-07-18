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
package com.couchbase.client.dcp;

import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.conductor.ConfigProvider;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.transport.netty.DcpConnectHandler;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.epoll.EpollSocketChannel;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.oio.OioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.channel.socket.oio.OioSocketChannel;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.functions.Func1;

import java.util.Arrays;
import java.util.List;

/**
 * Main entry point into the DCP client.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class Client {

    private final Conductor conductor;
    private final ClientEnvironment env;

    private Client(Builder builder) {
        if (builder.dataEventHandler == null) {
            throw new IllegalArgumentException("A DataEventHandler needs to be provided!");
        }

        EventLoopGroup eventLoopGroup = builder.eventLoopGroup == null
            ? new NioEventLoopGroup() : builder.eventLoopGroup;
        env = ClientEnvironment.builder()
            .setClusterAt(builder.clusterAt)
            .setConnectionNameGenerator(builder.connectionNameGenerator)
            .setBucket(builder.bucket)
            .setPassword(builder.password)
            .setDcpControl(builder.dcpControl)
            .setEventLoopGroup(eventLoopGroup)
            .setDataEventHandler(builder.dataEventHandler)
            .build();

        conductor = new Conductor(env, builder.configProvider);
    }

    public static Builder configure() {
        return new Builder();
    }

    /**
     * Connect the client and initialize everything as configured.
     */
    public Completable connect() {
        return conductor.connect();
    }

    /**
     * Shutdown the client and associated resources.
     */
    public Completable disconnect() {
        return conductor.stop();
    }

    /**
     * Start all partition streams from beginning, so all data in the bucket will be streamed.
     */
    public Completable startFromBeginning() {
        return Observable
            .range(0, conductor.numberOfPartitions())
            .flatMap(new Func1<Integer, Observable<?>>() {
                @Override
                public Observable<?> call(Integer p) {
                    return conductor.startStreamForPartition(p.shortValue()).toObservable();
                }
            })
            .toCompletable();
    }

    public static class Builder {
        private List<String> clusterAt = Arrays.asList("127.0.0.1");
        private DataEventHandler dataEventHandler;
        private EventLoopGroup eventLoopGroup;
        private String bucket = "default";
        private String password = "";
        private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
        private DcpControl dcpControl = new DcpControl();
        private ConfigProvider configProvider = null;

        public Builder clusterAt(final List<String> clusterAt) {
            this.clusterAt = clusterAt;
            return this;
        }

        public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
            return this;
        }

        public Builder dataEventHandler(final DataEventHandler dataEventHandler) {
            this.dataEventHandler = dataEventHandler;
            return this;
        }

        public Builder bucket(final String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder password(final String password) {
            this.password = password;
            return this;
        }

        public Builder connectionNameGenerator(final ConnectionNameGenerator connectionNameGenerator) {
            this.connectionNameGenerator = connectionNameGenerator;
            return this;
        }

        public Builder controlParam(final DcpControl.Names name, String value) {
            this.dcpControl.put(name, value);
            return this;
        }

        public Builder configProvider(final ConfigProvider configProvider) {
            this.configProvider = configProvider;
            return this;
        }

        public Client build() {
            return new Client(this);
        }
    }

}
