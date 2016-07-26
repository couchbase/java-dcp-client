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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.conductor.Conductor;
import com.couchbase.client.dcp.conductor.ConfigProvider;
import com.couchbase.client.dcp.conductor.DcpChannel;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.*;
import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.dcp.transport.netty.DcpConnectHandler;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
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
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Main entry point into the DCP client.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class Client {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(Client.class);

    private final Conductor conductor;
    private final ClientEnvironment env;
    private final boolean bufferAckEnabled;
    private final SessionState sessionState;

    private Client(Builder builder) {
        EventLoopGroup eventLoopGroup = builder.eventLoopGroup == null
            ? new NioEventLoopGroup() : builder.eventLoopGroup;
        env = ClientEnvironment.builder()
            .setClusterAt(builder.clusterAt)
            .setConnectionNameGenerator(builder.connectionNameGenerator)
            .setBucket(builder.bucket)
            .setPassword(builder.password)
            .setDcpControl(builder.dcpControl)
            .setEventLoopGroup(eventLoopGroup)
            .setBufferAckWatermark(builder.bufferAckWatermark)
            .build();
        sessionState = new SessionState();

        bufferAckEnabled = env.dcpControl().bufferAckEnabled();
        if (bufferAckEnabled) {
            if (env.bufferAckWatermark() == 0) {
                throw new IllegalArgumentException("BufferAckWatermark needs to be set if bufferAck is enabled.");
            }
        }
        conductor = new Conductor(env, builder.configProvider);
    }

    public void controlEventHandler(final ControlEventHandler controlEventHandler) {
        env.setControlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (DcpSnapshotMarkerMessage.is(event)) {
                    short partition = DcpSnapshotMarkerMessage.partition(event);
                    PartitionState ps = sessionState.get(partition);
                    ps.setSnapshotStartSeqno(DcpSnapshotMarkerMessage.startSeqno(event));
                    ps.setSnapshotEndSeqno(DcpSnapshotMarkerMessage.endSeqno(event));
                    sessionState.set(partition, ps);
                }

                // Forward event to user.
                controlEventHandler.onEvent(event);
            }
        });
    }

    public SessionState sessionState() {
        return sessionState;
    }

    public void dataEventHandler(final DataEventHandler dataEventHandler) {
        env.setDataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (DcpMutationMessage.is(event)) {
                    short partition = DcpMutationMessage.partition(event);
                    PartitionState ps = sessionState.get(partition);
                    ps.setStartSeqno(DcpMutationMessage.revisionSeqno(event));
                    sessionState.set(partition, ps);
                } else if (DcpDeletionMessage.is(event)) {
                    short partition = DcpDeletionMessage.partition(event);
                    PartitionState ps = sessionState.get(partition);
                    ps.setStartSeqno(DcpDeletionMessage.revisionSeqno(event));
                    sessionState.set(partition, ps);
                } else if (DcpExpirationMessage.is(event)) {
                    short partition = DcpExpirationMessage.partition(event);
                    PartitionState ps = sessionState.get(partition);
                    ps.setStartSeqno(DcpExpirationMessage.revisionSeqno(event));
                    sessionState.set(partition, ps);
                }

                // Forward event to user.
                dataEventHandler.onEvent(event);
            }
        });
    }

    public static Builder configure() {
        return new Builder();
    }

    /**
     * Connect the client and initialize everything as configured.
     */
    public Completable connect() {
        if (env.dataEventHandler() == null) {
            throw new IllegalArgumentException("A DataEventHandler needs to be provided!");
        }
        if (env.controlEventHandler() == null) {
            throw new IllegalArgumentException("A ControlEventHandler needs to be provided!");
        }
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
     *
     * For simplicity you can provide a list of vbucket IDS, but if none are provided all are used
     * automatically.
     */
    public Completable startFromBeginningWithNoEnd(Integer... vbids) {
        sessionState.intializeToBeginningWithNoEnd();

        List<Integer> partitions = new ArrayList<Integer>();
        if (vbids.length > 0) {
            partitions = Arrays.asList(vbids);
            LOGGER.info("Starting Stream against partitions {} with no end.", partitions);
        } else {
            int numPartitions = conductor.numberOfPartitions();
            LOGGER.info("Starting Stream against all {} partitions with no end.", numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(i);
            }
        }
        Collections.sort(partitions);

        return Observable
            .from(partitions)
            .flatMap(new Func1<Integer, Observable<?>>() {
                @Override
                public Observable<?> call(Integer p) {
                    PartitionState partitionState = sessionState.get(p);

                    return conductor.startStreamForPartition(
                        p.shortValue(),
                        partitionState.getUuid(),
                        partitionState.getStartSeqno(),
                        partitionState.getEndSeqno(),
                        partitionState.getSnapshotStartSeqno(),
                        partitionState.getSnapshotEndSeqno()
                    ).toObservable();
                }
            })
            .toCompletable()
            .doOnCompleted(new Action0() {
                @Override
                public void call() {
                    LOGGER.info("Requested streams initialized, starting to stream.");
                }
            });
    }

    public Completable stopStreams(Integer... vbids) {
        List<Integer> partitions = partitionsForVbids(conductor.numberOfPartitions(), vbids);
        return Observable
            .from(partitions)
            .flatMap(new Func1<Integer, Observable<?>>() {
                @Override
                public Observable<?> call(Integer p) {
                    return conductor.stopStreamForPartition(p.shortValue()).toObservable();
                }
            })
            .toCompletable();
    }

    public Completable getFailoverLogs(Integer... vbids) {
        List<Integer> partitions = partitionsForVbids(conductor.numberOfPartitions(), vbids);
        return Observable
            .from(partitions)
            .flatMap(new Func1<Integer, Observable<?>>() {
                @Override
                public Observable<?> call(Integer p) {
                    return conductor.getFailoverLog(p.shortValue()).toObservable();
                }
            })
            .toCompletable();
    }

    private static final List<Integer> partitionsForVbids(int numPartitions, Integer... vbids) {
        List<Integer> partitions = new ArrayList<Integer>();
        if (vbids.length > 0) {
            partitions = Arrays.asList(vbids);
        } else {
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(i);
            }
        }
        Collections.sort(partitions);
        return partitions;
    }

    public int numPartitions() {
        return conductor.numberOfPartitions();
    }

    public boolean streamIsOpen(int vbid) {
        return conductor.streamIsOpen((short) vbid);
    }

    /**
     * Acknowledge bytes read if the DcpControl.Names.CONNECTION_BUFFER_SIZE is set on bootstrap.
     *
     * The Producer will not specifically request Buffer Acknowledgement messages for any mutations or at any
     * intervals of time. Decisions about when to send Buffer Acknowledgement messages is up to the Consumer. It is
     * recommend however that Consumers send an acknowledgement after 50KB or 20% of the buffer has been processed.
     *
     * @param vbid the partition id.
     * @param numBytes the number of bytes to acknowledge.
     */
    public void acknowledgeBuffer(int vbid, int numBytes) {
        if (!bufferAckEnabled) {
            return;
        }
        conductor.acknowledgeBuffer((short) vbid, numBytes);
    }

    public void acknowledgeBuffer(ByteBuf buffer) {
        acknowledgeBuffer(MessageUtil.getVbucket(buffer), buffer.readableBytes());
    }


    public static class Builder {
        private List<String> clusterAt = Arrays.asList("127.0.0.1");
        private EventLoopGroup eventLoopGroup;
        private String bucket = "default";
        private String password = "";
        private ConnectionNameGenerator connectionNameGenerator = DefaultConnectionNameGenerator.INSTANCE;
        private DcpControl dcpControl = new DcpControl();
        private ConfigProvider configProvider = null;
        private int bufferAckWatermark;

        /**
         * The buffer acknowledge watermark in percent.
         *
         * @param watermark between 0 and 100, needs to be > 0 if flow control is enabled.
         */
        public Builder bufferAckWatermark(int watermark) {
            if (watermark > 100 || watermark < 0) {
                throw new IllegalArgumentException("The bufferAckWatermark is percents, so it needs to be between" +
                    " 0 and 100");
            }
            this.bufferAckWatermark = watermark;
            return this;
        }

        public Builder hostnames(final List<String> hostnames) {
            this.clusterAt = hostnames;
            return this;
        }

        public Builder hostnames(String... hostnames) {
            return hostnames(Arrays.asList(hostnames));
        }

        public Builder eventLoopGroup(final EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
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

        public Builder controlParam(final DcpControl.Names name, Object value) {
            this.dcpControl.put(name, value.toString());
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
