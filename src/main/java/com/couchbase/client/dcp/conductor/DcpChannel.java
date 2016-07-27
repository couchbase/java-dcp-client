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

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import com.couchbase.client.core.state.NotConnectedException;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.*;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultPromise;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import com.couchbase.client.deps.io.netty.util.concurrent.Promise;
import rx.*;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Logical representation of a DCP cluster connection.
 *
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel extends AbstractStateMachine<LifecycleState> {

    private static final AtomicInteger OPAQUE = new AtomicInteger(0);

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannel.class);

    private final ClientEnvironment env;
    private final InetAddress inetAddress;
    private final Subject<ByteBuf, ByteBuf> controlSubject;
    private final Map<Integer, Promise<?>> outstandingPromises;
    private final Map<Integer, Short> outstandingVbucketInfos;
    private volatile Channel channel;
    private final AtomicIntegerArray openStreams;
    private final boolean needsBufferAck;
    private final int bufferAckWatermark;
    private volatile int bufferAckCounter;

    public DcpChannel(InetAddress inetAddress, final ClientEnvironment env) {
        super(LifecycleState.DISCONNECTED);
        this.inetAddress = inetAddress;
        this.env = env;
        this.outstandingPromises = new ConcurrentHashMap<Integer, Promise<?>>();
        this.outstandingVbucketInfos = new ConcurrentHashMap<Integer, Short>();
        this.controlSubject = PublishSubject.<ByteBuf>create().toSerialized();
        this.openStreams = new AtomicIntegerArray(1024);
        this.needsBufferAck = env.dcpControl().bufferAckEnabled();

        this.bufferAckCounter = 0;
        if (needsBufferAck) {
            int bufferAckPercent = env.bufferAckWatermark();
            int bufferSize = Integer.parseInt(env.dcpControl().get(DcpControl.Names.CONNECTION_BUFFER_SIZE));
            this.bufferAckWatermark = (int) Math.round(bufferSize / 100.0 * bufferAckPercent);
            LOGGER.debug("BufferAckWatermark absolute is {}", bufferAckWatermark);
        } else {
            this.bufferAckWatermark = 0;
        }

        this.controlSubject
            .filter(new Func1<ByteBuf, Boolean>() {
                @Override
                public Boolean call(ByteBuf buf) {
                    if (DcpOpenStreamResponse.is(buf)) {
                        return filterOpenStreamResponse(buf);
                    } else if (DcpFailoverLogResponse.is(buf)) {
                        return filterFailoverLogResponse(buf);
                    } else if (DcpStreamEndMessage.is(buf)) {
                        return filterDcpStreamEndMessage(buf);
                    } else if (DcpCloseStreamResponse.is(buf)) {
                        return filterDcpCloseStreamResponse(buf);
                    } else if (DcpGetPartitionSeqnosResponse.is(buf)) {
                        return filterDcpGetPartitionSeqnosResponse(buf);
                    }
                    return true;
                }
            })
            .subscribe(new Subscriber<ByteBuf>() {
                @Override
                public void onCompleted() { /* Ignoring on purpose. */}

                @Override
                public void onError(Throwable e) { /* Ignoring on purpose. */ }

                @Override
                public void onNext(ByteBuf buf) {
                    env.controlEventHandler().onEvent(buf);
                }
            });
    }

    private boolean filterOpenStreamResponse(ByteBuf buf) {
        try {
            Promise promise = outstandingPromises.remove(MessageUtil.getOpaque(buf));
            short vbid = outstandingVbucketInfos.remove(MessageUtil.getOpaque(buf));
            short status = MessageUtil.getStatus(buf);
            switch (status) {
                case 0x00:
                    promise.setSuccess(null);
                    // create a failover log message and emit
                    ByteBuf flog = Unpooled.buffer();
                    DcpFailoverLogResponse.init(flog);
                    DcpFailoverLogResponse.vbucket(flog, DcpOpenStreamResponse.vbucket(buf));
                    MessageUtil.setContent(MessageUtil.getContent(buf).copy().writeShort(vbid), flog);
                    env.controlEventHandler().onEvent(flog);
                    break;
                case 0x23:
                    promise.setSuccess(null);
                    // create a rollback message and emit
                    ByteBuf rb = Unpooled.buffer();
                    RollbackMessage.init(rb, vbid, MessageUtil.getContent(buf).getLong(0));
                    env.controlEventHandler().onEvent(rb);
                    break;
                default:
                    promise.setFailure(new IllegalStateException("Unhandled Status: " + status));
            }
            return false;
        } finally {
            buf.release();
        }
    }

    private boolean filterDcpGetPartitionSeqnosResponse(ByteBuf buf) {
        try {
            Promise<ByteBuf> promise = (Promise<ByteBuf>) outstandingPromises.remove(MessageUtil.getOpaque(buf));
            promise.setSuccess(MessageUtil.getContent(buf).copy());
            return false;
        } finally {
            buf.release();
        }
    }

    private boolean filterFailoverLogResponse(ByteBuf buf) {
        try {
            Promise<ByteBuf> promise = (Promise<ByteBuf>) outstandingPromises.remove(MessageUtil.getOpaque(buf));
            short vbid = outstandingVbucketInfos.remove(MessageUtil.getOpaque(buf));

            ByteBuf flog = Unpooled.buffer();
            DcpFailoverLogResponse.init(flog);
            DcpFailoverLogResponse.vbucket(flog, DcpFailoverLogResponse.vbucket(buf));
            MessageUtil.setContent(MessageUtil.getContent(buf).copy().writeShort(vbid), flog);
            promise.setSuccess(flog);
            return false;
        } finally {
            buf.release();
        }
    }

    private boolean filterDcpStreamEndMessage(ByteBuf buf) {
        try {
            int flag = MessageUtil.getExtras(buf).readInt();
            short vbid = DcpStreamEndMessage.vbucket(buf);
            LOGGER.debug("Server closed Stream on vbid {} with flag {}", vbid, flag);
            openStreams.set(vbid, 0);
            if (needsBufferAck) {
                acknowledgeBuffer(buf.readableBytes());
            }
            return false;
        } finally {
            buf.release();
        }
    }

    private boolean filterDcpCloseStreamResponse(ByteBuf buf) {
        try {
            Promise<?> promise = outstandingPromises.remove(MessageUtil.getOpaque(buf));
            promise.setSuccess(null);
            if (needsBufferAck) {
                acknowledgeBuffer(buf.readableBytes());
            }
            return false;
        } finally {
            buf.release();
        }
    }

    public Completable connect() {
        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                if (state() != LifecycleState.DISCONNECTED) {
                    subscriber.onCompleted();
                    return;
                }
                final Bootstrap bootstrap = new Bootstrap()
                    .remoteAddress(inetAddress, 11210)
                    .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
                    .handler(new DcpPipeline(env, controlSubject))
                    .group(env.eventLoopGroup());

                transitionState(LifecycleState.CONNECTING);
                bootstrap.connect().addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            channel = future.channel();
                            transitionState(LifecycleState.CONNECTED);
                            LOGGER.info("Connected to Node {}", channel.remoteAddress());
                            subscriber.onCompleted();
                        } else {
                            transitionState(LifecycleState.DISCONNECTED);
                            LOGGER.warn("IMPLEMENT ME!!! (retry on failure until removed)");
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    public Completable disconnect() {
        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                if (channel != null) {
                    transitionState(LifecycleState.DISCONNECTING);
                    bufferAckCounter = 0;
                    channel.close().addListener(new GenericFutureListener<ChannelFuture>() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            transitionState(LifecycleState.DISCONNECTED);
                            LOGGER.info("Disconnected from Node " + hostname());
                            if (future.isSuccess()) {
                                subscriber.onCompleted();
                            } else {
                                LOGGER.debug("Error during channel close.", future.cause());
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

    public InetAddress hostname() {
        return inetAddress;
    }


    public void acknowledgeBuffer(final int numBytes) {
        if (state() != LifecycleState.CONNECTED) {
            throw new NotConnectedException(new NotConnectedException());
        }

        LOGGER.trace("Acknowledging {} bytes against connection {}.", numBytes, channel.remoteAddress());

        bufferAckCounter += numBytes;

        LOGGER.trace("BufferAckCounter is now {}", bufferAckCounter);

        if (bufferAckCounter >= bufferAckWatermark) {
            LOGGER.trace("BufferAckWatermark reached on {}, acking now against the server.", channel.remoteAddress());
            ByteBuf buffer = Unpooled.buffer();
            DcpBufferAckRequest.init(buffer);
            DcpBufferAckRequest.ackBytes(buffer, bufferAckCounter);
            channel.writeAndFlush(buffer);
            bufferAckCounter = 0;
        }
    }

    public Completable openStream(final short vbid, final long vbuuid, final long startSeqno, final long endSeqno,
                                  final long snapshotStartSeqno, final long snapshotEndSeqno) {
        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                if (state() != LifecycleState.CONNECTED) {
                    subscriber.onError(new NotConnectedException());
                    return;
                }

                LOGGER.debug("Opening Stream against {} with vbid: {}, vbuuid: {}, startSeqno: {}, " +
                    "endSeqno: {},  snapshotStartSeqno: {}, snapshotEndSeqno: {}",
                    channel.remoteAddress(), vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);

                int opaque = OPAQUE.incrementAndGet();
                ChannelPromise promise = channel.newPromise();

                ByteBuf buffer = Unpooled.buffer();
                DcpOpenStreamRequest.init(buffer, vbid);
                DcpOpenStreamRequest.opaque(buffer, opaque);
                DcpOpenStreamRequest.vbuuid(buffer, vbuuid);
                DcpOpenStreamRequest.startSeqno(buffer, startSeqno);
                DcpOpenStreamRequest.endSeqno(buffer, endSeqno);
                DcpOpenStreamRequest.snapshotStartSeqno(buffer, snapshotStartSeqno);
                DcpOpenStreamRequest.snapshotEndSeqno(buffer, snapshotEndSeqno);

                outstandingPromises.put(opaque, promise);
                outstandingVbucketInfos.put(opaque, vbid);
                channel.writeAndFlush(buffer);

                promise.addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            LOGGER.debug("Opened Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
                            openStreams.set(vbid, 1);
                            subscriber.onCompleted();
                        } else {
                            LOGGER.debug("Failed open Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
                            openStreams.set(vbid, 0);
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    public Completable closeStream(final short vbid) {
        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                if (state() != LifecycleState.CONNECTED) {
                    subscriber.onError(new NotConnectedException());
                    return;
                }

                LOGGER.debug("Closing Stream against {} with vbid: {}", channel.remoteAddress(), vbid);

                int opaque = OPAQUE.incrementAndGet();
                ChannelPromise promise = channel.newPromise();

                ByteBuf buffer = Unpooled.buffer();
                DcpCloseStreamRequest.init(buffer);
                DcpCloseStreamRequest.vbucket(buffer, vbid);
                DcpCloseStreamRequest.opaque(buffer, opaque);

                outstandingPromises.put(opaque, promise);
                channel.writeAndFlush(buffer);

                promise.addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        openStreams.set(vbid, 0);
                        if (future.isSuccess()) {
                            LOGGER.debug("Closed Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
                            subscriber.onCompleted();
                        } else {
                            LOGGER.debug("Failed close Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    /**
     * Returns all seqnos for all vbuckets on that channel.
     */
    public Single<ByteBuf> getSeqnos() {
        return Single.create(new Single.OnSubscribe<ByteBuf>() {
            @Override
            public void call(final SingleSubscriber<? super ByteBuf> subscriber) {
                if (state() != LifecycleState.CONNECTED) {
                    subscriber.onError(new NotConnectedException());
                    return;
                }

                int opaque = OPAQUE.incrementAndGet();
                Promise<ByteBuf> promise = new DefaultPromise<ByteBuf>(channel.eventLoop());

                ByteBuf buffer = Unpooled.buffer();
                DcpGetPartitionSeqnosRequest.init(buffer);
                DcpGetPartitionSeqnosRequest.opaque(buffer, opaque);

                outstandingPromises.put(opaque, promise);
                channel.writeAndFlush(buffer);

                promise.addListener(new GenericFutureListener<Future<ByteBuf>>() {
                    @Override
                    public void operationComplete(Future<ByteBuf> future) throws Exception {
                        if (future.isSuccess()) {
                            subscriber.onSuccess(future.getNow());
                        } else {
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    public Single<ByteBuf> getFailoverLog(final short vbid) {
        return Single.create(new Single.OnSubscribe<ByteBuf>() {
            @Override
            public void call(final SingleSubscriber<? super ByteBuf> subscriber) {
                if (state() != LifecycleState.CONNECTED) {
                    subscriber.onError(new NotConnectedException());
                    return;
                }

                int opaque = OPAQUE.incrementAndGet();
                Promise<ByteBuf> promise = new DefaultPromise<ByteBuf>(channel.eventLoop());

                ByteBuf buffer = Unpooled.buffer();
                DcpFailoverLogRequest.init(buffer);
                DcpFailoverLogRequest.opaque(buffer, opaque);
                DcpFailoverLogRequest.vbucket(buffer, vbid);

                outstandingPromises.put(opaque, promise);
                outstandingVbucketInfos.put(opaque, vbid);
                channel.writeAndFlush(buffer);


                promise.addListener(new GenericFutureListener<Future<ByteBuf>>() {
                    @Override
                    public void operationComplete(Future<ByteBuf> future) throws Exception {
                        if (future.isSuccess()) {
                            LOGGER.debug("Asked for failover log on {} for vbid: {}", channel.remoteAddress(), vbid);
                            subscriber.onSuccess(future.getNow());
                        } else {
                            LOGGER.debug("Failed to ask for failover log on {} for vbid: {}", channel.remoteAddress(), vbid);
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    public boolean streamIsOpen(short vbid) {
        return openStreams.get(vbid) == 1;
    }

    @Override
    public boolean equals(Object o) {
        return inetAddress.equals(o);
    }

    @Override
    public int hashCode() {
        return inetAddress.hashCode();
    }

    @Override
    public String toString() {
        return "DcpChannel{inetAddress=" + inetAddress + '}';
    }
}
