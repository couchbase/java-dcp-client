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
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.ConfigPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The {@link HttpStreamingConfigProvider}s only purpose is to keep new configs coming in all the time in a resilient manner.
 *
 * Todo: attach a listener to the channel close event and try to rebootstrap as long as not stopped.
 *
 * @author Michael Nitschinger
 */
public class HttpStreamingConfigProvider implements ConfigProvider {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(HttpStreamingConfigProvider.class);

    private final AtomicReference<List<String>> remoteHosts;
    private final Subject<CouchbaseBucketConfig, CouchbaseBucketConfig> configStream;
    private volatile boolean stopped = false;
    private volatile Channel channel;
    private final ClientEnvironment env;

    public HttpStreamingConfigProvider(ClientEnvironment env) {
        this.env = env;
        this.remoteHosts = new AtomicReference<List<String>>(env.clusterAt());
        this.configStream = PublishSubject.<CouchbaseBucketConfig>create().toSerialized();

        configStream.doOnNext(new Action1<CouchbaseBucketConfig>() {
            @Override
            public void call(CouchbaseBucketConfig config) {
                List<String> newNodes = new ArrayList<String>();
                for (NodeInfo node : config.nodes()) {
                    newNodes.add(node.hostname().getHostAddress());
                }
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
        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                if (stopped) {
                    subscriber.onCompleted();
                    return;
                }

                stopped = true;
                if (channel != null) {
                    channel.close().addListener(new GenericFutureListener<ChannelFuture>() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                subscriber.onCompleted();
                            } else {
                                subscriber.onError(future.cause());
                            }
                        }
                    });
                    channel.close();
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
        final Bootstrap bootstrap = new Bootstrap()
            .remoteAddress(hostname, 8091)
            .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
            .handler(new ConfigPipeline(hostname, env.bucket(), env.password(), configStream))
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
                                    channel = null;
                                    if (!stopped) {
                                        tryConnectHosts().subscribe();
                                    }
                                }
                            });
                            LOGGER.debug("Successfully established config connection to Socket {}", channel.remoteAddress());
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
