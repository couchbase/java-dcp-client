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
import com.couchbase.client.core.message.observe.Observe;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.internal.ConcurrentSet;
import rx.*;
import rx.Observable;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

    private final ConfigProvider configProvider;
    private final Set<DcpChannel> channels;
    private volatile long configRev = -1;
    private final ClientEnvironment env;
    private final AtomicReference<CouchbaseBucketConfig> currentConfig;
    private final boolean ownsConfigProvider;

    public Conductor(final ClientEnvironment env, ConfigProvider cp) {
        this.env = env;
        this.currentConfig = new AtomicReference<CouchbaseBucketConfig>();

        configProvider = cp == null ? new HttpStreamingConfigProvider(env) : cp;
        ownsConfigProvider = cp == null;

        configProvider.configs().forEach(new Action1<CouchbaseBucketConfig>() {
            @Override
            public void call(CouchbaseBucketConfig config) {
                if (config.rev() > configRev) {
                    configRev = config.rev();
                    LOGGER.trace("Applying new configuration, rev is now {}.", configRev);
                    currentConfig.set(config);
                    reconfigure(config);
                } else {
                 LOGGER.trace("Ignoring config, since rev has not changed.");
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
        LOGGER.debug("Instructed to shutdown.");

       return Observable
            .from(channels)
            .flatMap(new Func1<DcpChannel, Observable<?>>() {
                @Override
                public Observable<?> call(DcpChannel dcpChannel) {
                    return dcpChannel.disconnect().toObservable();
                }
            })
            .flatMap(new Func1<Object, Observable<?>>() {
                @Override
                public Observable<?> call(Object o) {
                    if (ownsConfigProvider) {
                        return configProvider.stop().toObservable();
                    } else {
                        return Observable.just(1);
                    }
                }
            })
           .doOnCompleted(new Action0() {
               @Override
               public void call() {
                   LOGGER.info("Shutdown complete.");
               }
           })
            .toCompletable();
    }

    /**
     * Returns the total number of partitions.
     */
    public int numberOfPartitions() {
        CouchbaseBucketConfig config = currentConfig.get();
        return config.numberOfPartitions();
    }

    public Observable<ByteBuf> getSeqnos() {
        return Observable
            .from(channels)
            .flatMap(new Func1<DcpChannel, Observable<ByteBuf>>() {
                @Override
                public Observable<ByteBuf> call(DcpChannel channel) {
                    return getSeqnosForChannel(channel);
                }
            });
    }

    private Observable<ByteBuf> getSeqnosForChannel(final DcpChannel channel) {
        if (channel.state() != LifecycleState.CONNECTED) {
            LOGGER.debug("Rescheduling get Seqnos for channel {}, not connected (yet).", channel);
            return Observable
                .timer(100, TimeUnit.MILLISECONDS)
                .flatMap(new Func1<Long, Observable<ByteBuf>>() {
                    @Override
                    public Observable<ByteBuf> call(Long aLong) {
                        return getSeqnosForChannel(channel);
                    }
                });
        }
        return channel.getSeqnos().toObservable();
    }

    public Single<ByteBuf> getFailoverLog(final short partition) {
        final DcpChannel channel = masterChannelByPartition(partition);
        if (channel.state() != LifecycleState.CONNECTED) {
            return Observable.timer(100, TimeUnit.MILLISECONDS)
                .flatMap(new Func1<Long, Observable<ByteBuf>>() {
                    @Override
                    public Observable<ByteBuf> call(Long aLong) {
                        return getFailoverLog(partition).toObservable();
                    }
                }).first().toSingle();
        }
        return channel.getFailoverLog(partition);
    }

    public Completable startStreamForPartition(final short partition, final long vbuuid, final long startSeqno,
        final long endSeqno, final long snapshotStartSeqno, final long snapshotEndSeqno) {
        DcpChannel channel = masterChannelByPartition(partition);
        if (channel.state() != LifecycleState.CONNECTED) {
            LOGGER.debug("Rescheduling Stream Start for vbid {}, not connected (yet).", partition);
            return Observable
                .timer(100, TimeUnit.MILLISECONDS)
                .flatMap(new Func1<Long, Observable<?>>() {
                    @Override
                    public Observable<?> call(Long aLong) {
                        return startStreamForPartition(partition, vbuuid, startSeqno, endSeqno,
                            snapshotStartSeqno, snapshotEndSeqno).toObservable();
                    }
                }).toCompletable();
        }
        return channel.openStream(partition, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);
    }

    public Completable stopStreamForPartition(final short partition) {
        DcpChannel channel = masterChannelByPartition(partition);
        return channel.closeStream(partition);
    }

    public boolean streamIsOpen(final short partition) {
        DcpChannel channel = masterChannelByPartition(partition);
        return channel.streamIsOpen(partition);
    }

    public void acknowledgeBuffer(final short partition, int numBytes) {
        DcpChannel channel = masterChannelByPartition(partition);
        channel.acknowledgeBuffer(numBytes);
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

            boolean in = false;
            for (DcpChannel chan : channels) {
                if (chan.hostname().equals(hostname)) {
                    in = true;
                    break;
                }
            }
            if (!in) {
                toAdd.add(hostname);
                LOGGER.debug("Planning to add {}", hostname);
            }
        }

        for (DcpChannel chan : channels) {
            boolean found = false;
            for (NodeInfo node : config.nodes()) {
                InetAddress hostname = node.hostname();
                if (hostname.equals(chan.hostname())) {
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

        LOGGER.debug("Adding DCP Channel against {}", node);
        DcpChannel channel = new DcpChannel(node, env);
        channels.add(channel);
        channel.connect().subscribe();
    }

    private void remove(DcpChannel node) {
        if(channels.remove(node)) {
            LOGGER.debug("Removing DCP Channel against {}", node);
            node.disconnect().subscribe();
       }
    }
}
