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
package com.couchbase.client.dcp.config;

import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Stateful environment for internal usage.
 */
public class ClientEnvironment {

    private final List<String> clusterAt;
    private final ConnectionNameGenerator connectionNameGenerator;
    private final String bucket;
    private final String password;
    private final DcpControl dcpControl;
    private final EventLoopGroup eventLoopGroup;
    private final boolean eventLoopGroupIsPrivate;

    private final int bufferAckWatermark;

    private volatile DataEventHandler dataEventHandler;
    private volatile ControlEventHandler controlEventHandler;

    private ClientEnvironment(Builder builder) {
        clusterAt = builder.clusterAt;
        connectionNameGenerator = builder.connectionNameGenerator;
        bucket = builder.bucket;
        password = builder.password;
        dcpControl = builder.dcpControl;
        eventLoopGroup = builder.eventLoopGroup;
        eventLoopGroupIsPrivate = builder.eventLoopGroupIsPrivate;
        bufferAckWatermark = builder.bufferAckWatermark;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> clusterAt() {
        return clusterAt;
    }

    public DataEventHandler dataEventHandler() {
        return dataEventHandler;
    }

    public ControlEventHandler controlEventHandler() {
        return controlEventHandler;
    }

    public ConnectionNameGenerator connectionNameGenerator() {
        return connectionNameGenerator;
    }

    public String bucket() {
        return bucket;
    }

    public String password() {
        return password;
    }

    public DcpControl dcpControl() {
        return dcpControl;
    }

    public int bufferAckWatermark() {
        return bufferAckWatermark;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public void setDataEventHandler(DataEventHandler dataEventHandler) {
        this.dataEventHandler = dataEventHandler;
    }

    public void setControlEventHandler(ControlEventHandler controlEventHandler) {
        this.controlEventHandler = controlEventHandler;
    }

    public static class Builder {
        private List<String> clusterAt;
        private ConnectionNameGenerator connectionNameGenerator;
        private String bucket;
        private String password;
        private DcpControl dcpControl;
        private EventLoopGroup eventLoopGroup;
        private boolean eventLoopGroupIsPrivate;

        private int bufferAckWatermark;

        public Builder setClusterAt(List<String> clusterAt) {
            this.clusterAt = clusterAt;
            return this;
        }

        public Builder setBufferAckWatermark(int watermark) {
            this.bufferAckWatermark = watermark;
            return this;
        }

        public Builder setConnectionNameGenerator(ConnectionNameGenerator connectionNameGenerator) {
            this.connectionNameGenerator = connectionNameGenerator;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDcpControl(DcpControl dcpControl) {
            this.dcpControl = dcpControl;
            return this;
        }

        public Builder setEventLoopGroup(EventLoopGroup eventLoopGroup, boolean priv) {
            this.eventLoopGroup = eventLoopGroup;
            this.eventLoopGroupIsPrivate = priv;
            return this;
        }

        public ClientEnvironment build() {
            return new ClientEnvironment(this);
        }

    }

    public Completable shutdown() {
        if (!eventLoopGroupIsPrivate) {
            return Completable.complete();
        }

        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS).addListener(new GenericFutureListener() {
                    @Override
                    public void operationComplete(Future future) throws Exception {
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

}
