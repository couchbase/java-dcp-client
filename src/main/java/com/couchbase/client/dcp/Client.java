/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.core.utils.ConnectionString;
import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.conductor.ConfigProvider;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.error.BootstrapException;
import com.couchbase.client.dcp.error.RollbackException;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpExpirationMessage;
import com.couchbase.client.dcp.message.DcpFailoverLogResponse;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.util.MathUtils;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.channel.nio.NioEventLoopGroup;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;

import rx.Completable;
import rx.CompletableSubscriber;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.functions.Func2;

/**
 * This {@link Client} provides the main API to configure and use the DCP client.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class Client {

    /**
     * The logger used.
     */
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Client.class);

    /**
     * The {@link Conductor} handles channels and streams. It's the orchestrator of everything.
     */
    private final Conductor conductor;

    /**
     * The stateful {@link ClientEnvironment}, used internally for centralized config management.
     */
    private final ClientEnvironment env;

    /**
     * If buffer acknowledgment is enabled.
     */
    private final boolean bufferAckEnabled;

    /**
     * Creates a new {@link Client} instance.
     *
     * @param builder the client config builder.
     */
    private Client(Builder builder) {
        EventLoopGroup eventLoopGroup = builder.eventLoopGroup == null
                ? new NioEventLoopGroup() : builder.eventLoopGroup;

        env = ClientEnvironment.builder()
                .setClusterAt(builder.clusterAt)
                .setConnectionNameGenerator(builder.connectionNameGenerator)
                .setBucket(builder.bucket)
                .setUsername(builder.username == null ? builder.bucket : builder.username)
                .setPassword(builder.password)
                .setDcpControl(builder.dcpControl)
                .setEventLoopGroup(eventLoopGroup, builder.eventLoopGroup == null)
                .setBufferAckWatermark(builder.bufferAckWatermark)
                .setBufferPooling(builder.poolBuffers)
                .setConnectTimeout(builder.connectTimeout)
                .setBootstrapTimeout(builder.bootstrapTimeout)
                .setSocketConnectTimeout(builder.socketConnectTimeout)
                .setConfigProviderReconnectDelay(builder.configProviderReconnectDelay)
                .setConfigProviderReconnectMaxAttempts(builder.configProviderReconnectMaxAttempts)
                .setDcpChannelsReconnectDelay(builder.dcpChannelsReconnectDelay)
                .setDcpChannelsReconnectMaxAttempts(builder.dcpChannelsReconnectMaxAttempts)
                .setEventBus(builder.eventBus)
                .setSslEnabled(builder.sslEnabled)
                .setSslKeystoreFile(builder.sslKeystoreFile)
                .setSslKeystorePassword(builder.sslKeystorePassword)
                .setSslKeystore(builder.sslKeystore)
                .build();

        bufferAckEnabled = env.dcpControl().bufferAckEnabled();
        if (bufferAckEnabled) {
            if (env.bufferAckWatermark() == 0) {
                throw new IllegalArgumentException("The bufferAckWatermark needs to be set if bufferAck is enabled.");
            }
        }

        conductor = new Conductor(env, builder.configProvider);
        LOGGER.info("Environment Configuration Used: {}", env);
    }

    /**
     * Allows to configure the {@link Client} before bootstrap through a {@link Builder}.
     *
     * @return the builder to configure the client.
     */
    public static Builder configure() {
        return new Builder();
    }

    /**
     * Get the current sequence numbers from all partitions.
     *
     * Each element emitted into the observable has two elements. The first element is the partition and
     * the second element is its sequence number.
     *
     * @return an {@link Observable} of sequence number arrays.
     */
    private Observable<long[]> getSeqnos() {
        return conductor.getSeqnos().flatMap(new Func1<ByteBuf, Observable<long[]>>() {
            @Override
            public Observable<long[]> call(ByteBuf buf) {
                int numPairs = buf.readableBytes() / 10; // 2 byte short + 8 byte long
                List<long[]> pairs = new ArrayList<long[]>(numPairs);
                for (int i = 0; i < numPairs; i++) {
                    pairs.add(new long[]{buf.getShort(10 * i), buf.getLong(10 * i + 2)});
                }
                buf.release();
                return Observable.from(pairs);
            }
        });
    }

    /**
     * Returns the current {@link SessionState}, useful for persistence and inspection.
     *
     * @return the current session state.
     */
    public SessionState sessionState() {
        return conductor.sessionState();
    }

    /**
     * Stores a {@link ControlEventHandler} to be called when control events happen.
     *
     * All events (passed as {@link ByteBuf}s) that the callback receives need to be handled
     * and at least released (by using {@link ByteBuf#release()}, otherwise they will leak.
     *
     * The following messages can happen and should be handled depending on the needs of the
     * client:
     *
     * - {@link RollbackMessage}: If during a connect phase the server responds with rollback
     * information, this event is forwarded to the callback. Does not need to be acknowledged.
     *
     * - {@link DcpSnapshotMarkerRequest}: Server transmits data in batches called snapshots
     * before sending anything, it send marker message, which contains start and end sequence
     * numbers of the data in it. Need to be acknowledged.
     *
     * Keep in mind that the callback is executed on the IO thread (netty's thread pool for the
     * event loops) so further synchronization is needed if the data needs to be used on a different
     * thread in a thread safe manner.
     *
     * @param controlEventHandler the event handler to use.
     */
    public void controlEventHandler(final ControlEventHandler controlEventHandler) {
        env.setControlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                if (DcpSnapshotMarkerRequest.is(event)) {
                    // Keep snapshot information in the session state, but also forward event to user
                    short partition = DcpSnapshotMarkerRequest.partition(event);
                    PartitionState ps = sessionState().get(partition);
                    ps.setSnapshotStartSeqno(DcpSnapshotMarkerRequest.startSeqno(event));
                    ps.setSnapshotEndSeqno(DcpSnapshotMarkerRequest.endSeqno(event));
                    sessionState().set(partition, ps);
                } else if (DcpFailoverLogResponse.is(event)) {
                    // Do not forward failover log responses for now since their info is kept
                    // in the session state transparently
                    handleFailoverLogResponse(event);
                    event.release();
                    return;
                } else if (RollbackMessage.is(event)) {
                    // even if forwarded to the user, warn in case the user is not
                    // aware of rollback messages.
                    LOGGER.warn(
                            "Received rollback for vbucket {} to seqno {}",
                            RollbackMessage.vbucket(event),
                            RollbackMessage.seqno(event)
                    );
                }

                // Forward event to user.
                controlEventHandler.onEvent(flowController, event);
            }
        });
    }

    /**
     * Stores a {@link SystemEventHandler} to be called when control events happen.
     */
    public void systemEventHandler(final SystemEventHandler systemEventHandler) {
        env.setSystemEventHandler(systemEventHandler);
    }

    /**
     * Helper method to handle a failover log response.
     *
     * @param event the buffer representing the {@link DcpFailoverLogResponse}.
     */
    private void handleFailoverLogResponse(final ByteBuf event) {
        short partition = DcpFailoverLogResponse.vbucket(event);
        int numEntries = DcpFailoverLogResponse.numLogEntries(event);
        PartitionState ps = sessionState().get(partition);
        for (int i = 0; i < numEntries; i++) {
            ps.addToFailoverLog(
                    DcpFailoverLogResponse.seqnoEntry(event, i),
                    DcpFailoverLogResponse.vbuuidEntry(event, i)
            );
        }
        sessionState().set(partition, ps);
    }

    /**
     * Stores a {@link DataEventHandler} to be called when data events happen.
     *
     * All events (passed as {@link ByteBuf}s) that the callback receives need to be handled
     * and at least released (by using {@link ByteBuf#release()}, otherwise they will leak.
     *
     * The following messages can happen and should be handled depending on the needs of the
     * client:
     *
     * - {@link DcpMutationMessage}: A mtation has occurred. Needs to be acknowledged.
     * - {@link DcpDeletionMessage}: A deletion has occurred. Needs to be acknowledged.
     * - {@link DcpExpirationMessage}: An expiration has occurred. Note that current server versions
     * (as of 4.5.0) are not emitting this event, but in any case you should at least release it to
     * be forwards compatible. Needs to be acknowledged.
     *
     * Keep in mind that the callback is executed on the IO thread (netty's thread pool for the
     * event loops) so further synchronization is needed if the data needs to be used on a different
     * thread in a thread safe manner.
     *
     * @param dataEventHandler the event handler to use.
     */
    public void dataEventHandler(final DataEventHandler dataEventHandler) {
        env.setDataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                if (DcpMutationMessage.is(event)) {
                    short partition = DcpMutationMessage.partition(event);
                    PartitionState ps = sessionState().get(partition);
                    ps.setStartSeqno(DcpMutationMessage.bySeqno(event));
                    sessionState().set(partition, ps);
                } else if (DcpDeletionMessage.is(event)) {
                    short partition = DcpDeletionMessage.partition(event);
                    PartitionState ps = sessionState().get(partition);
                    ps.setStartSeqno(DcpDeletionMessage.bySeqno(event));
                    sessionState().set(partition, ps);
                } else if (DcpExpirationMessage.is(event)) {
                    short partition = DcpExpirationMessage.partition(event);
                    PartitionState ps = sessionState().get(partition);
                    ps.setStartSeqno(DcpExpirationMessage.bySeqno(event));
                    sessionState().set(partition, ps);
                }

                // Forward event to user.
                dataEventHandler.onEvent(flowController, event);
            }
        });
    }

    /**
     * Initializes the underlying connections (not the streams) and sets up everything as needed.
     *
     * @return a {@link Completable} signaling that the connect phase has been completed or failed.
     */
    public Completable connect() {
        if (!conductor.disconnected()) {
            // short-circuit connect attempt if the conductor is already connecting/connected.
            LOGGER.debug("Ignoring duplicate connect attempt, already connecting/connected.");
            return Completable.complete();
        }

        if (env.dataEventHandler() == null) {
            throw new IllegalArgumentException("A DataEventHandler needs to be provided!");
        }
        if (env.controlEventHandler() == null) {
            throw new IllegalArgumentException("A ControlEventHandler needs to be provided!");
        }
        LOGGER.info("Connecting to seed nodes and bootstrapping bucket {}.", env.bucket());
        return conductor.connect().onErrorResumeNext(new Func1<Throwable, Completable>() {
            @Override
            public Completable call(Throwable throwable) {
                return conductor.stop()
                        .andThen(Completable.error(new BootstrapException("Could not connect to Cluster/Bucket", throwable)));
            }
        });
    }

    /**
     * Disconnect the {@link Client} and shut down all its owned resources.
     *
     * If custom state is used (like a shared {@link EventLoopGroup}), then they must be closed and managed
     * separately after this disconnect process has finished.
     *
     * @return a {@link Completable} signaling that the disconnect phase has been completed or failed.
     */
    public Completable disconnect() {
        return conductor.stop().andThen(env.shutdown());
    }

    /**
     * Start DCP streams based on the initialized state for the given partition IDs (vbids).
     *
     * If no ids are provided, all initialized partitions will be started.
     *
     * @param vbids the partition ids (0-indexed) to start streaming for.
     * @return a {@link Completable} indicating that streaming has started or failed.
     */
    public Completable startStreaming(Short... vbids) {
        int numPartitions = numPartitions();
        final List<Short> partitions = partitionsForVbids(numPartitions, vbids);

        List<Short> initializedPartitions = selectInitializedPartitions(numPartitions, partitions);
        if (initializedPartitions.isEmpty()) {
            LOGGER.info("The configured session state does not require any streams to be opened. Completing immediately.");
            return Completable.complete();
        }

        LOGGER.info("Starting to Stream for " + initializedPartitions.size() + " partitions");
        LOGGER.debug("Stream start against partitions: {}", initializedPartitions);

        return Observable
                .from(initializedPartitions)
                .flatMap(new Func1<Short, Observable<?>>() {
                    @Override
                    public Observable<?> call(Short partition) {
                        PartitionState partitionState = sessionState().get(partition);
                        return conductor.startStreamForPartition(
                                partition,
                                partitionState.getLastUuid(),
                                partitionState.getStartSeqno(),
                                partitionState.getEndSeqno(),
                                partitionState.getSnapshotStartSeqno(),
                                partitionState.getSnapshotEndSeqno()
                        ).onErrorResumeNext(new Func1<Throwable, Completable>() {
                            @Override
                            public Completable call(Throwable throwable) {
                                if (throwable instanceof RollbackException) {
                                    // We ignore rollbacks since they are handled out of band by the user.
                                    return Completable.complete();
                                } else {
                                    return Completable.error(throwable);
                                }
                            }
                        }).toObservable();
                    }
                })
                .toCompletable();
    }

    /**
     * Helper method to check on stream start that some kind of state is initialized to avoid a common error
     * of starting without initializing.
     */
    private List<Short> selectInitializedPartitions(int clusterPartitions, List<Short> partitions) {
        ArrayList<Short> initializedPartitions = new ArrayList<Short>();
        SessionState state = sessionState();

        for (short partition : partitions) {
            PartitionState ps = state.get(partition);
            if (ps != null) {
                if (MathUtils.lessThanUnsigned(ps.getStartSeqno(), ps.getEndSeqno())) {
                    initializedPartitions.add(partition);
                } else {
                    LOGGER.debug("Skipping partition {}, because startSeqno({}) >= endSeqno({})",
                            partition, ps.getStartSeqno(), ps.getEndSeqno());
                }
            } else {
                LOGGER.debug("Skipping partition {}, because its state is null", partition);
            }
        }

        if (initializedPartitions.size() > clusterPartitions) {
            throw new IllegalStateException("Session State has " + initializedPartitions
                    + " partitions while the cluster has " + clusterPartitions + "!");
        }
        return initializedPartitions;
    }

    /**
     * Stop DCP streams for the given partition IDs (vbids).
     *
     * If no ids are provided, all partitions will be stopped. Note that you can also use this to "pause" streams
     * if {@link #startStreaming(Short...)} is called later - since the session state is persisted and streaming
     * will resume from the current position.
     *
     * @param vbids the partition ids (0-indexed) to stop streaming for.
     * @return a {@link Completable} indicating that streaming has stopped or failed.
     */
    public Completable stopStreaming(Short... vbids) {
        List<Short> partitions = partitionsForVbids(numPartitions(), vbids);

        LOGGER.info("Stopping to Stream for " + partitions.size() + " partitions");
        LOGGER.debug("Stream stop against partitions: {}", partitions);

        return Observable
                .from(partitions)
                .flatMap(new Func1<Short, Observable<?>>() {
                    @Override
                    public Observable<?> call(Short p) {
                        return conductor.stopStreamForPartition(p).toObservable();
                    }
                })
                .toCompletable();
    }

    /**
     * Helper method to turn the array of vbids into a list.
     *
     * @param numPartitions the number of partitions on the cluster as a fallback.
     * @param vbids         the potentially empty array of selected vbids.
     * @return a sorted list of partitions to use.
     */
    private static List<Short> partitionsForVbids(int numPartitions, Short... vbids) {
        List<Short> partitions = new ArrayList<Short>();
        if (vbids.length > 0) {
            partitions = Arrays.asList(vbids);
        } else {
            for (short i = 0; i < numPartitions; i++) {
                partitions.add(i);
            }
        }
        Collections.sort(partitions);
        return partitions;
    }

    /**
     * Helper method to return the failover logs for the given partitions (vbids).
     *
     * If the list is empty, the failover logs for all partitions will be returned. Note that the returned
     * ByteBufs can be analyzed using the {@link DcpFailoverLogResponse} flyweight.
     *
     * @param vbids the partitions to return the failover logs from.
     * @return an {@link Observable} containing all failover logs.
     */
    public Observable<ByteBuf> failoverLogs(Short... vbids) {
        List<Short> partitions = partitionsForVbids(numPartitions(), vbids);

        LOGGER.debug("Asking for failover logs on partitions {}", partitions);

        return Observable
                .from(partitions)
                .flatMap(new Func1<Short, Observable<ByteBuf>>() {
                    @Override
                    public Observable<ByteBuf> call(Short p) {
                        return conductor.getFailoverLog(p).toObservable();
                    }
                });
    }

    /**
     * Helper method to rollback the partition state and stop/restart the stream.
     *
     * The stream is stopped (if not already done). Then:
     *
     * The rollback seqno state is applied. Note that this will also remove all the failover logs for the partition
     * that are higher than the given seqno, since the server told us we are ahead of it.
     *
     * Finally, the stream is restarted again.
     *
     * @param partition the partition id
     * @param seqno     the sequence number to rollback to
     */
    public Completable rollbackAndRestartStream(final short partition, final long seqno) {
        return stopStreaming(partition)
                .andThen(Completable.create(new Completable.OnSubscribe() {
                    @Override
                    public void call(CompletableSubscriber subscriber) {
                        sessionState().rollbackToPosition(partition, seqno);
                        subscriber.onCompleted();
                    }
                }))
                .andThen(startStreaming(partition));
    }


    /**
     * Returns the number of partitions on the remote cluster.
     *
     * Note that you must be connected, since the information is loaded form the server configuration.
     * On all OS'es other than OSX it will be 1024, on OSX it is 64. Treat this as an opaque value anyways.
     *
     * @return the number of partitions (vbuckets).
     */
    public int numPartitions() {
        return conductor.numberOfPartitions();
    }


    /**
     * Returns true if the stream for the given partition id is currently open.
     *
     * @param vbid the partition id.
     * @return true if it is open, false otherwise.
     */
    public boolean streamIsOpen(short vbid) {
        return conductor.streamIsOpen(vbid);
    }

    /**
     * Initialize the {@link SessionState} based on arbitrary time points.
     *
     * The following combinations are supported and make sense:
     *
     * - {@link StreamFrom#BEGINNING} to {@link StreamTo#NOW}
     * - {@link StreamFrom#BEGINNING} to {@link StreamTo#INFINITY}
     * - {@link StreamFrom#NOW} to {@link StreamTo#INFINITY}
     *
     * If you already have state captured and you want to resume from this position, use
     * {@link #recoverState(StateFormat, byte[])} or {@link #recoverOrInitializeState(StateFormat, byte[], StreamFrom, StreamTo)}
     * instead.
     *
     * @param from where to start streaming from.
     * @param to   when to stop streaming.
     * @return A {@link Completable} indicating the success or failure of the state init.
     */
    public Completable initializeState(final StreamFrom from, final StreamTo to) {
        if (from == StreamFrom.BEGINNING && to == StreamTo.INFINITY) {
            buzzMe();
            return initFromBeginningToInfinity();
        } else if (from == StreamFrom.BEGINNING && to == StreamTo.NOW) {
            return initFromBeginningToNow();
        } else if (from == StreamFrom.NOW && to == StreamTo.INFINITY) {
            buzzMe();
            return initFromNowToInfinity();
        } else {
            throw new IllegalStateException("Unsupported FROM/TO combination: " + from + " -> " + to);
        }
    }

    /**
     * Initializes the {@link SessionState} from a previous snapshot with specific state information.
     *
     * If a system needs to be built that withstands outages and needs to resume where left off, this method,
     * combined with the periodic persistence of the {@link SessionState} provides resume capabilities. If you
     * need to start fresh, take a look at {@link #initializeState(StreamFrom, StreamTo)} as well as
     * {@link #recoverOrInitializeState(StateFormat, byte[], StreamFrom, StreamTo)}.
     *
     * @param format         the format used when persisting.
     * @param persistedState the opaque byte array representing the persisted state.
     * @return A {@link Completable} indicating the success or failure of the state recovery.
     */
    public Completable recoverState(final StateFormat format, final byte[] persistedState) {
        return Completable.create(new Completable.OnSubscribe() {
            @Override
            public void call(CompletableSubscriber subscriber) {
                LOGGER.info("Recovering state from format {}", format);
                LOGGER.debug("PersistedState on recovery is: {}", new String(persistedState, CharsetUtil.UTF_8));

                try {
                    if (format == StateFormat.JSON) {
                        sessionState().setFromJson(persistedState);
                        subscriber.onCompleted();
                    } else {
                        subscriber.onError(new IllegalStateException("Unsupported StateFormat " + format));
                    }
                } catch (Exception ex) {
                    subscriber.onError(ex);
                }
            }
        });
    }

    /**
     * Recovers or initializes the {@link SessionState}.
     *
     * This method is a convience wrapper around initialization and recovery. It combines both methods and
     * checks if the persisted state byte array is null or empty and if so it starts with the params given. If
     * it is not empty it recovers from there. This acknowledges the fact that ideally the state is persisted
     * somewhere but if its not there you want to start at a specific point in time.
     *
     * @param format         the persistence format used.
     * @param persistedState the state, may be null or empty.
     * @param from           from where to start streaming if persisted state is null or empty.
     * @param to             to where to stream if persisted state is null or empty.
     * @return A {@link Completable} indicating the success or failure of the state recovery or init.
     */
    public Completable recoverOrInitializeState(final StateFormat format, final byte[] persistedState,
                                                final StreamFrom from, final StreamTo to) {
        if (persistedState == null || persistedState.length == 0) {
            return initializeState(from, to);
        } else {
            return recoverState(format, persistedState);
        }
    }


    /**
     * Initializes the session state from beginning to no end.
     */
    private Completable initFromBeginningToInfinity() {
        return Completable.create(new Completable.OnSubscribe() {
            @Override
            public void call(CompletableSubscriber subscriber) {
                LOGGER.info("Initializing state from beginning to no end.");

                try {
                    sessionState().setToBeginningWithNoEnd(numPartitions());
                    subscriber.onCompleted();
                } catch (Exception ex) {
                    LOGGER.warn("Failed to initialize state from beginning to no end.", ex);
                    subscriber.onError(ex);
                }
            }
        });
    }

    /**
     * Initializes the session state from now to no end.
     */
    private Completable initFromNowToInfinity() {
        return initWithCallback(new Action1<long[]>() {
            @Override
            public void call(long[] longs) {
                short partition = (short) longs[0];
                long seqno = longs[1];
                PartitionState partitionState = sessionState().get(partition);
                partitionState.setStartSeqno(seqno);
                partitionState.setSnapshotStartSeqno(seqno);
                partitionState.setSnapshotEndSeqno(seqno);
                sessionState().set(partition, partitionState);
            }
        });
    }

    /**
     * Initializes the session state from beginning to now.
     */
    private Completable initFromBeginningToNow() {
        return initWithCallback(new Action1<long[]>() {
            @Override
            public void call(long[] longs) {
                short partition = (short) longs[0];
                long seqno = longs[1];
                PartitionState partitionState = sessionState().get(partition);
                partitionState.setEndSeqno(seqno);
                sessionState().set(partition, partitionState);
            }
        });
    }

    /**
     * Helper method to initialize all kinds of states.
     *
     * This method grabs the sequence numbers and then calls a callback for customization. Once that is done it
     * grabs the failover logs and populates the session state with the failover log information.
     */
    private Completable initWithCallback(Action1<long[]> callback) {
        sessionState().setToBeginningWithNoEnd(numPartitions());

        return getSeqnos()
                .doOnNext(callback)
                .reduce(new ArrayList<Short>(), new Func2<ArrayList<Short>, long[], ArrayList<Short>>() {
                    @Override
                    public ArrayList<Short> call(ArrayList<Short> shorts, long[] longs) {
                        shorts.add((short) longs[0]);
                        return shorts;
                    }
                })
                .flatMap(new Func1<ArrayList<Short>, Observable<ByteBuf>>() {
                    @Override
                    public Observable<ByteBuf> call(ArrayList<Short> shorts) {
                        return failoverLogs(shorts.toArray(new Short[]{}));
                    }
                })
                .map(new Func1<ByteBuf, Short>() {
                    @Override
                    public Short call(ByteBuf buf) {
                        short partition = DcpFailoverLogResponse.vbucket(buf);
                        handleFailoverLogResponse(buf);
                        buf.release();
                        return partition;
                    }
                }).last().toCompletable();
    }

    /**
     * Builder object to customize the {@link Client} creation.
     */
    public static class Builder {
        private List<InetSocketAddress> clusterAt = Arrays.asList(InetSocketAddress.createUnresolved("127.0.0.1", 0));;
        private EventLoopGroup eventLoopGroup;
        private String bucket = "default";
        private String username;
        private String password = "";
        private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
        private DcpControl dcpControl = new DcpControl();
        private ConfigProvider configProvider = null;
        private int bufferAckWatermark;
        private boolean poolBuffers = true;
        private long connectTimeout = ClientEnvironment.DEFAULT_SOCKET_CONNECT_TIMEOUT;
        private long bootstrapTimeout = ClientEnvironment.DEFAULT_BOOTSTRAP_TIMEOUT;
        private long socketConnectTimeout = ClientEnvironment.DEFAULT_SOCKET_CONNECT_TIMEOUT;
        private Delay configProviderReconnectDelay = ClientEnvironment.DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY;
        private int configProviderReconnectMaxAttempts = ClientEnvironment.DEFAULT_CONFIG_PROVIDER_RECONNECT_MAX_ATTEMPTS;
        private int dcpChannelsReconnectMaxAttempts = ClientEnvironment.DEFAULT_DCP_CHANNELS_RECONNECT_MAX_ATTEMPTS;
        private Delay dcpChannelsReconnectDelay = ClientEnvironment.DEFAULT_DCP_CHANNELS_RECONNECT_DELAY;
        private EventBus eventBus;
        private boolean sslEnabled = ClientEnvironment.DEFAULT_SSL_ENABLED;
        private String sslKeystoreFile;
        private String sslKeystorePassword;
        private KeyStore sslKeystore;

        /**
         * The buffer acknowledge watermark in percent.
         *
         * @param watermark between 0 and 100, needs to be > 0 if flow control is enabled.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder bufferAckWatermark(int watermark) {
            if (watermark > 100 || watermark < 0) {
                throw new IllegalArgumentException("The bufferAckWatermark is percents, so it needs to be between" +
                        " 0 and 100");
            }
            this.bufferAckWatermark = watermark;
            return this;
        }

        /**
         * The hostnames to bootstrap against.
         *
         * @param hostnames seed nodes.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder hostnames(final List<String> hostnames) {
            this.clusterAt = new ArrayList<InetSocketAddress>(hostnames.size());
            for (String hostname : hostnames) {
                this.clusterAt.add(new InetSocketAddress(hostname, 0));
            }
            return this;
        }

        /**
         * The hostnames to bootstrap against.
         *
         * @param hostnames seed nodes.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder hostnames(String... hostnames) {
            return hostnames(Arrays.asList(hostnames));
        }

        /**
         * Connection string to bootstrap with.
         *
         * Note: it overrides list of hostnames defined by {@link #hostnames(List)}.
         *
         * Connection string specification defined in SDK-RFC-11:
         * https://github.com/couchbaselabs/sdk-rfcs/blob/master/rfc/0011-connection-string.md
         *
         * @param connectionString seed nodes.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder connectionString(String connectionString) {
            ConnectionString cs = ConnectionString.create(connectionString);
            this.clusterAt = cs.hosts();
            return this;
        }

        /**
         * Sets a custom event loop group, this is needed if more than one client is initialized and
         * runs at the same time to keep the IO threads efficient and in bounds.
         *
         * @param eventLoopGroup the group that should be used.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
            return this;
        }

        /**
         * The name of the bucket to use.
         *
         * @param bucket name of the bucket
         * @return this {@link Builder} for nice chainability.
         */
        public Builder bucket(final String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder username(final String username){
            this.username = username;
            return this;
        }

        /**
         * The password of the bucket to use.
         *
         * @param password the password.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder password(final String password) {
            this.password = password;
            return this;
        }

        /**
         * If specific names for DCP connections should be generated, a custom one can be provided.
         *
         * @param connectionNameGenerator custom generator.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder connectionNameGenerator(final ConnectionNameGenerator connectionNameGenerator) {
            this.connectionNameGenerator = connectionNameGenerator;
            return this;
        }

        /**
         * Set all kinds of DCP control params - check their description for more information.
         *
         * @param name  the name of the param
         * @param value the value of the param
         * @return this {@link Builder} for nice chainability.
         */
        public Builder controlParam(final DcpControl.Names name, Object value) {
            this.dcpControl.put(name, value.toString());
            return this;
        }

        /**
         * A custom configuration provider can be shared and passed in across clients. use with care!
         *
         * @param configProvider the custom config provider.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder configProvider(final ConfigProvider configProvider) {
            this.configProvider = configProvider;
            return this;
        }

        /**
         * If buffer pooling should be enabled (yes by default).
         *
         * @param pool enable or disable buffer pooling.
         * @return this {@link Builder} for nice chainability.
         */
        public Builder poolBuffers(final boolean pool) {
            this.poolBuffers = pool;
            return this;
        }

        /**
         * Sets a custom socket connect timeout.
         *
         * @param socketConnectTimeout the socket connect timeout in milliseconds.
         */
        public Builder socketConnectTimeout(long socketConnectTimeout) {
            this.socketConnectTimeout = socketConnectTimeout;
            return this;
        }

        /**
         * Time to wait for first configuration during bootstrap.
         *
         * @param bootstrapTimeout time in milliseconds.
         */
        public Builder bootstrapTimeout(long bootstrapTimeout) {
            this.bootstrapTimeout = bootstrapTimeout;
            return this;
        }

        /**
         * Time to wait configuration provider socket to connect.
         *
         * @param connectTimeout time in milliseconds.
         */
        public Builder connectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Delay between retry attempts for configuration provider
         *
         * @param configProviderReconnectDelay
         */
        public Builder configProviderReconnectDelay(Delay configProviderReconnectDelay) {
            this.configProviderReconnectDelay = configProviderReconnectDelay;
            return this;
        }


        /**
         * The maximum number of reconnect attempts for configuration provider
         *
         * @param configProviderReconnectMaxAttempts
         */
        public Builder configProviderReconnectMaxAttempts(int configProviderReconnectMaxAttempts) {
            this.configProviderReconnectMaxAttempts = configProviderReconnectMaxAttempts;
            return this;
        }

        /**
         * The maximum number of reconnect attempts for DCP channels
         *
         * @param dcpChannelsReconnectMaxAttempts
         */
        public Builder dcpChannelsReconnectMaxAttempts(int dcpChannelsReconnectMaxAttempts) {
            this.dcpChannelsReconnectMaxAttempts = dcpChannelsReconnectMaxAttempts;
            return this;
        }

        /**
         * Delay between retry attempts for DCP channels
         *
         * @param dcpChannelsReconnectDelay
         */
        public Builder dcpChannelsReconnectDelay(Delay dcpChannelsReconnectDelay) {
            this.dcpChannelsReconnectDelay = dcpChannelsReconnectDelay;
            return this;
        }

        /**
         * Sets the event bus to an alternative implementation.
         *
         * This setting should only be tweaked in advanced cases.
         */
        public Builder eventBus(final EventBus eventBus) {
            this.eventBus = eventBus;
            return this;
        }


        /**
         * Set if SSL should be enabled (default value {@value ClientEnvironment#DEFAULT_SSL_ENABLED}).
         * If true, also set {@link #sslKeystoreFile(String)} and {@link #sslKeystorePassword(String)}.
         */
        public Builder sslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        /**
         * Defines the location of the SSL Keystore file (default value null, none).
         *
         * You can either specify a file or the keystore directly via {@link #sslKeystore(KeyStore)}. If the explicit
         * keystore is used it takes precedence over the file approach.
         */
        public Builder sslKeystoreFile(final String sslKeystoreFile) {
            this.sslKeystoreFile = sslKeystoreFile;
            return this;
        }

        /**
         * Sets the SSL Keystore password to be used with the Keystore file (default value null, none).
         *
         * @see #sslKeystoreFile(String)
         */
        public Builder sslKeystorePassword(final String sslKeystorePassword) {
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        /**
         * Sets the SSL Keystore directly and not indirectly via filepath.
         *
         * You can either specify a file or the keystore directly via {@link #sslKeystore(KeyStore)}. If the explicit
         * keystore is used it takes precedence over the file approach.
         *
         * @param sslKeystore the keystore to use.
         */
        public Builder sslKeystore(final KeyStore sslKeystore) {
            this.sslKeystore = sslKeystore;
            return this;
        }

        /**
         * Create the client instance ready to use.
         *
         * @return the built client instance.
         */
        public Client build() {
            return new Client(this);
        }
    }

    /**
     *            _._                           _._
     *           ||||                           ||||
     *           ||||_           ___           _||||
     *           |  ||        .-'___`-.        ||  |
     *           \   /      .' .'_ _'. '.      \   /
     *           /~~|       | (| b d |) |       |~~\
     *          /'  |       |  |  '  |  |       |  `\
     *,        /__.-:      ,|  | `-' |  |,      :-.__\       ,
     *|'-------(    \-''""/.|  /\___/\  |.\""''-/    )------'|
     *|         \_.-'\   /   '-._____.-'   \   /'-._/        |
     *|.---------\   /'._| _    .---. ===  |_.'\   /--------.|
     *'           \ /  | |\_\ _ \=v=/  _   | |  \ /          '
     *             `.  | | \_\_\ ~~~  (_)  | |  .'
     *               `'"'|`'--.__.^.__.--'`|'"'`
     *                   \                 /
     *                    `,..---'"'---..,'
     *                      :--..___..--:    TO INFINITY...
     *                       \         /
     *                       |`.     .'|       AND BEYOND!
     *                       |  :___:  |
     *                       |   | |   |
     *                       |   | |   |
     *                       |.-.| |.-.|
     *                       |`-'| |`-'|
     *                       |   | |   |
     *                      /    | |    \
     *                     |_____| |_____|
     *                     ':---:-'-:---:'
     *                     /    |   |    \
     *                jgs /.---.|   |.---.\
     *                    `.____;   :____.'
     */
    private static void buzzMe() {
        LOGGER.debug("To Infinity... AND BEYOND!");
    }

}
