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
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.ConfigPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.PooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelOption;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action4;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.dcp.util.retry.RetryBuilder.any;

/**
 * The {@link HttpStreamingConfigProvider}s only purpose is to keep new configs coming in all the time in a resilient manner.
 *
 *
 * @author Michael Nitschinger
 */
public class HttpStreamingConfigProvider extends AbstractStateMachine<LifecycleState> implements ConfigProvider {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(HttpStreamingConfigProvider.class);

    private final AtomicReference<List<String>> remoteHosts;
    private final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream;
    private volatile boolean stopped = false;
    private volatile Channel channel;
    private final ClientEnvironment env;

    public HttpStreamingConfigProvider(ClientEnvironment env) {
        super(LifecycleState.DISCONNECTED);
        this.env = env;
        this.remoteHosts = new AtomicReference<List<String>>(env.clusterAt());
        this.configStream = BehaviorSubject.<CouchbaseBucketConfig>create().toSerialized();

        configStream.subscribe(new Subscriber<CouchbaseBucketConfig>() {
            @Override
            public void onCompleted() {
                LOGGER.debug("Config stream completed.");
            }

            @Override
            public void onError(Throwable e) {
                LOGGER.warn("Error on config stream!", e);
            }

            @Override
            public void onNext(CouchbaseBucketConfig config) {
                List<String> newNodes = new ArrayList<String>();
                for (NodeInfo node : config.nodes()) {
                    newNodes.add(node.hostname().getHostAddress());
                }

                LOGGER.trace("Updated config stream node list to {}.", newNodes);
                remoteHosts.set(newNodes);
            }
        });
    }

    @Override
    public Completable start() {
       return tryConnectHosts();
    }

    @Override
    public Completable stop() {
        stopped = true;

        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                LOGGER.debug("Initiating streaming config provider shutdown on channel.");
                transitionState(LifecycleState.DISCONNECTING);
                if (channel != null) {
                    Channel ch = channel;
                    channel = null;
                    ch.close().addListener(new GenericFutureListener<ChannelFuture>() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            transitionState(LifecycleState.DISCONNECTED);
                            if (future.isSuccess()) {
                                LOGGER.debug("Streaming config provider channel shutdown completed.");
                                subscriber.onCompleted();
                            } else {
                                LOGGER.warn("Error during streaming config provider shutdown!", future.cause());
                                subscriber.onError(future.cause());
                            }
                        }
                    });
                } else {
                    subscriber.onCompleted();
                }
            }
        });
    }

    @Override
    public Observable<CouchbaseBucketConfig> configs() {
        return configStream;
    }

    private Completable tryConnectHosts() {
        if (stopped) {
            LOGGER.debug("Not trying to connect to hosts, already stopped.");
            return Completable.complete();
        }

        transitionState(LifecycleState.CONNECTING);
        List<String> hosts = remoteHosts.get();
        Completable chain = tryConnectHost(hosts.get(0));
        for (int i = 1; i < hosts.size(); i++) {
            final String h = hosts.get(i);
            chain = chain.onErrorResumeNext(new Func1<Throwable, Completable>() {
                @Override
                public Completable call(Throwable throwable) {
                    LOGGER.warn("Could not get config from Node, trying next in list.", throwable);
                    return tryConnectHost(h);
                }
            });
        }
        return chain;
    }

    private Completable tryConnectHost(final String hostname) {
        ByteBufAllocator allocator = env.poolBuffers()
            ? PooledByteBufAllocator.DEFAULT : UnpooledByteBufAllocator.DEFAULT;
        final Bootstrap bootstrap = new Bootstrap()
            .remoteAddress(hostname, env.sslEnabled() ? env.bootstrapHttpSslPort() : env.bootstrapHttpDirectPort())
            .option(ChannelOption.ALLOCATOR, allocator)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int)env.socketConnectTimeout())
            .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
            .handler(new ConfigPipeline(env, hostname, configStream))
            .group(env.eventLoopGroup());

        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                bootstrap.connect().addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            channel = future.channel();
                            channel.closeFuture().addListener(new GenericFutureListener<ChannelFuture>() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    transitionState(LifecycleState.DISCONNECTED);
                                    channel = null;
                                    triggerReconnect();
                                }
                            });
                            LOGGER.debug("Successfully established config connection to Socket {}", channel.remoteAddress());
                            transitionState(LifecycleState.CONNECTED);
                            subscriber.onCompleted();
                        } else {
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    private void triggerReconnect() {
        transitionState(LifecycleState.CONNECTING);
        if (!stopped) {
            tryConnectHosts()
                .retryWhen(any()
                    .delay(env.configProviderReconnectDelay())
                    .max(env.configProviderReconnectMaxAttempts())
                    .doOnRetry(new Action4<Integer, Throwable, Long, TimeUnit>() {
                        @Override
                        public void call(Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) {
                            LOGGER.info("No host usable to fetch a config from, waiting and retrying (remote hosts: {}).",
                                remoteHosts.get());
                        }
                    })
                    .build()
                )
                .subscribe();
        }
    }

}
