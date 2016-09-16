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
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.internal.ConcurrentSet;
import rx.*;
import rx.Observable;
import rx.functions.*;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.dcp.util.retry.RetryBuilder.any;
import static com.couchbase.client.dcp.util.retry.RetryBuilder.anyOf;

public class Conductor {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Conductor.class);

    private final ConfigProvider configProvider;
    private final Set<DcpChannel> channels;
    private volatile long configRev = -1;
    private final ClientEnvironment env;
    private final AtomicReference<CouchbaseBucketConfig> currentConfig;
    private final boolean ownsConfigProvider;
    private final SessionState sessionState;

    public Conductor(final ClientEnvironment env, ConfigProvider cp) {
        this.env = env;
        this.currentConfig = new AtomicReference<CouchbaseBucketConfig>();
        sessionState = new SessionState();

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

    public SessionState sessionState() {
        return sessionState;
    }

    public Completable connect() {
        Completable atLeastOneConfig = configProvider.configs().first().toCompletable();
        return configProvider.start().concatWith(atLeastOneConfig);
    }

    public Completable stop() {
        LOGGER.debug("Instructed to shutdown.");

       Completable channelShutdown = Observable
            .from(channels)
            .flatMap(new Func1<DcpChannel, Observable<?>>() {
                @Override
                public Observable<?> call(DcpChannel dcpChannel) {
                    return dcpChannel.disconnect().toObservable();
                }
            })
            .toCompletable();

        if (ownsConfigProvider) {
            channelShutdown = channelShutdown.andThen(configProvider.stop());
        }

       return channelShutdown.doOnCompleted(new Action0() {
           @Override
           public void call() {
               LOGGER.info("Shutdown complete.");
           }
       });
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
        return Observable
            .just(channel)
            .flatMap(new Func1<DcpChannel, Observable<ByteBuf>>() {
                @Override
                public Observable<ByteBuf> call(DcpChannel channel) {
                    return channel.getSeqnos().toObservable();
                }
            })
            .retryWhen(anyOf(NotConnectedException.class)
                .max(Integer.MAX_VALUE)
                .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
                .doOnRetry(new Action4<Integer, Throwable, Long, TimeUnit>() {
                    @Override
                    public void call(Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) {
                        LOGGER.debug("Rescheduling get Seqnos for channel {}, not connected (yet).", channel);

                    }
                })
                .build()
            );
    }

    public Single<ByteBuf> getFailoverLog(final short partition) {
        return Observable
            .just(partition)
            .map(new Func1<Short, DcpChannel>() {
                @Override
                public DcpChannel call(Short aShort) {
                    return masterChannelByPartition(partition);
                }
            })
            .flatMap(new Func1<DcpChannel, Observable<ByteBuf>>() {
                @Override
                public Observable<ByteBuf> call(DcpChannel channel) {
                    return channel.getFailoverLog(partition).toObservable();
                }
            })
            .retryWhen(anyOf(NotConnectedException.class)
                .max(Integer.MAX_VALUE)
                .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
                .doOnRetry(new Action4<Integer, Throwable, Long, TimeUnit>() {
                    @Override
                    public void call(Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) {
                        LOGGER.debug("Rescheduling Get Failover Log for vbid {}, not connected (yet).", partition);

                    }
                })
                .build()
            ).toSingle();
    }

    public Completable startStreamForPartition(final short partition, final long vbuuid, final long startSeqno,
        final long endSeqno, final long snapshotStartSeqno, final long snapshotEndSeqno) {
        return Observable
            .just(partition)
            .map(new Func1<Short, DcpChannel>() {
                @Override
                public DcpChannel call(Short aShort) {
                    return masterChannelByPartition(partition);
                }
            })
            .flatMap(new Func1<DcpChannel, Observable<?>>() {
                @Override
                public Observable<?> call(DcpChannel channel) {
                    return channel.openStream(partition, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno)
                        .toObservable();
                }
            })
            .retryWhen(anyOf(NotConnectedException.class)
                .max(Integer.MAX_VALUE)
                .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
                .doOnRetry(new Action4<Integer, Throwable, Long, TimeUnit>() {
                    @Override
                    public void call(Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) {
                        LOGGER.debug("Rescheduling Stream Start for vbid {}, not connected (yet).", partition);

                    }
                })
                .build()
            )
            .toCompletable();
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

            if (!in && config.hasPrimaryPartitionsOnNode(hostname)) {
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
        DcpChannel channel = new DcpChannel(node, env, this);
        channels.add(channel);

        channel
            .connect()
            .retryWhen(any().max(Integer.MAX_VALUE).delay(Delay.fixed(200, TimeUnit.MILLISECONDS)).doOnRetry(new Action4<Integer, Throwable, Long, TimeUnit>() {
                @Override
                public void call(Integer integer, Throwable throwable, Long aLong, TimeUnit timeUnit) {
                    LOGGER.debug("Rescheduling Node reconnect for DCP channel {}", node);
                }
            }).build())
            .subscribe(new Completable.CompletableSubscriber() {
                @Override
                public void onCompleted() {
                    LOGGER.debug("Completed Node connect for DCP channel {}", node);
                }

                @Override
                public void onError(Throwable e) {
                    LOGGER.warn("Got error during connect (maybe retried) for node {}" + node, e);
                }

                @Override
                public void onSubscribe(Subscription d) {
                    // ignored.
                }
            });
    }

    private void remove(final DcpChannel node) {
        if(channels.remove(node)) {
            LOGGER.debug("Removing DCP Channel against {}", node);
            node.disconnect().subscribe(new Completable.CompletableSubscriber() {
                @Override
                public void onCompleted() {
                    LOGGER.debug("Channel remove notified as complete for {}", node.hostname());
                }

                @Override
                public void onError(Throwable e) {
                    LOGGER.warn("Got error during Node removal for node {}" + node.hostname(), e);
                }

                @Override
                public void onSubscribe(Subscription d) {
                    // ignored.
                }
            });
       }
    }

    /**
     * Called by the {@link DcpChannel} to signal a stream end done by the server and it
     * most likely needs to be moved over to a new node during rebalance/failover.
     *
     * @param partition the partition to move if needed
     */
    void maybeMovePartition(final short partition) {
        Observable
            .timer(50, TimeUnit.MILLISECONDS)
            .filter(new Func1<Long, Boolean>() {
                @Override
                public Boolean call(Long aLong) {
                    PartitionState ps = sessionState.get(partition);
                    boolean desiredSeqnoReached = ps.isAtEnd();
                    if (desiredSeqnoReached) {
                        LOGGER.debug("Reached desired high seqno {} for vbucket {}, not reopening stream.",
                            ps.getEndSeqno(), partition);
                    }
                    return !desiredSeqnoReached;
                }
            })
            .flatMap(new Func1<Long, Observable<?>>() {
                @Override
                public Observable<?> call(Long aLong) {
                    PartitionState ps = sessionState.get(partition);
                    return startStreamForPartition(
                        partition,
                        ps.getLastUuid(),
                        ps.getStartSeqno(),
                        ps.getEndSeqno(),
                        ps.getSnapshotStartSeqno(),
                        ps.getSnapshotEndSeqno()
                    ).retryWhen(anyOf(NotMyVbucketException.class)
                        .max(Integer.MAX_VALUE)
                        .delay(Delay.fixed(200, TimeUnit.MILLISECONDS))
                        .build()).toObservable();
                }
            }).toCompletable().subscribe(new Completable.CompletableSubscriber() {
                @Override
                public void onCompleted() {
                    LOGGER.trace("Completed Partition Move for partition {}", partition);
                }

                @Override
                public void onError(Throwable e) {
                    LOGGER.warn("Error during Partition Move for partition " + partition, e);
                }

                @Override
                public void onSubscribe(Subscription d) {
                    LOGGER.debug("Subscribing for Partition Move for partition {}", partition);
                }
            });
    }

}
