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

import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.transport.netty.DcpConnectHandler;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.socket.nio.NioSocketChannel;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.Single;

/**
 * Main entry point into the DCP client.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class Client {

    private final String clusterAt;
    private final DataEventHandler dataEventHandler;
    private final ConnectionNameGenerator connectionNameGenerator;
    private final String bucket;
    private final String password;
    private final DcpControl dcpControl;


    private Client(Builder builder) {
        clusterAt = builder.clusterAt;
        bucket = builder.bucket;
        password = builder.password;
        connectionNameGenerator = builder.connectionNameGenerator;
        dcpControl = builder.dcpControl;

        if (builder.dataEventHandler == null) {
            throw new IllegalArgumentException("A DataEventHandler needs to be provided!");
        }
        dataEventHandler = builder.dataEventHandler;
    }

    public static Builder configure() {
        return new Builder();
    }

    /**
     * Connect the client and initialize everything as configured.
     */
    public Completable connect() {

        final Bootstrap bootstrap = new Bootstrap()
            .remoteAddress(clusterAt, 11210)
            .channel(NioSocketChannel.class)
            .handler(new DcpPipeline(connectionNameGenerator, bucket, password, dcpControl))
            .group(new NioEventLoopGroup());


        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                bootstrap.connect().addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if (future.isSuccess()) {
                            subscriber.onCompleted();
                        } else {
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    /**
     * Shutdown the client and associated resources.
     */
    public Single<Boolean> disconnect() {
        return null;
    }

    public static class Builder {
        private String clusterAt = "127.0.0.1";
        private DataEventHandler dataEventHandler;
        private String bucket = "default";
        private String password = "";
        private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
        private DcpControl dcpControl = new DcpControl();

        public Builder clusterAt(final String clusterAt) {
            this.clusterAt = clusterAt;
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

        public Client build() {
            return new Client(this);
        }
    }

}
