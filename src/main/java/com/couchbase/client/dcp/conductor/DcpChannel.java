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
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
import com.couchbase.client.dcp.message.DcpOpenStreamResponse;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.channel.ChannelPromise;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Logical representation of a DCP cluster connection.
 *
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel {

    private static final AtomicInteger OPAQUE = new AtomicInteger(0);

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannel.class);

    private final ClientEnvironment env;
    private final InetAddress inetAddress;
    private final Subject<ByteBuf, ByteBuf> controlSubject;
    private final Map<Integer, ChannelPromise> outstandingResponses;
    private volatile Channel channel;

    public DcpChannel(InetAddress inetAddress, final ClientEnvironment env) {
        this.inetAddress = inetAddress;
        this.env = env;
        this.outstandingResponses = new ConcurrentHashMap<Integer, ChannelPromise>();
        this.controlSubject = PublishSubject.<ByteBuf>create().toSerialized();

        this.controlSubject
            .filter(new Func1<ByteBuf, Boolean>() {
                @Override
                public Boolean call(ByteBuf buf) {
                    if (DcpOpenStreamResponse.is(buf)) {
                        ChannelPromise promise = outstandingResponses.remove(MessageUtil.getOpaque(buf));
                        // TODO: what happens if not successful? set failure with an exception or so...
                        promise.setSuccess();
                        buf.release();
                        return false;
                    }

                    return true;
                }
            })
            .subscribe(new Subscriber<ByteBuf>() {
                @Override
                public void onCompleted() {
                    // Ignoring on purpose.
                }

                @Override
                public void onError(Throwable e) {
                    // Ignoring on purpose.
                }

                @Override
                public void onNext(ByteBuf buf) {
                    env.controlEventHandler().onEvent(buf);
                }
            });
    }

    public void connect() {
        final Bootstrap bootstrap = new Bootstrap()
            .remoteAddress(inetAddress, 11210)
            .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
            .handler(new DcpPipeline(env, controlSubject))
            .group(env.eventLoopGroup());

        bootstrap.connect().addListener(new GenericFutureListener<ChannelFuture>() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    channel = future.channel();
                } else {
                    LOGGER.warn("IMPLEMENT ME!!! (retry on failure until removed)");
                }
            }
        });
    }

    public void disconnect() {
        if (channel != null) {
            channel.close().addListener(new GenericFutureListener<ChannelFuture>() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        LOGGER.debug("Error during channel close.", future.cause());
                    }
                }
            });
        }
    }

    public InetAddress hostname() {
        return inetAddress;
    }


    public Completable openStream(final short vbid, final long vbuuid, final long startSeqno, final long endSeqno,
                                  final long snapshotStartSeqno, final long snapshotEndSeqno) {
        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                LOGGER.debug("Opening Stream against {} with vbid: {}, vbuuid: {}, startSeqno: {}, " +
                    "endSeqno: {},  snapshotStartSeqno: {}, snapshotEndSeqno: {}",
                    channel.remoteAddress(), vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);

                int opaque = OPAQUE.incrementAndGet();
                ChannelPromise promise = channel.newPromise();

                ByteBuf buffer = Unpooled.buffer();
                DcpOpenStreamRequest.init(buffer, vbid);
                DcpOpenStreamRequest.opaque(buffer, opaque);

                outstandingResponses.put(opaque, promise);
                channel.writeAndFlush(buffer);

                promise.addListener(new GenericFutureListener<ChannelFuture>() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            LOGGER.debug("Opened Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
                            subscriber.onCompleted();
                        } else {
                            LOGGER.debug("Failed open Stream against {} with vbid: {}", channel.remoteAddress(), vbid);
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    public Completable closeStream(short vbid) {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        return inetAddress.equals(o);
    }

    @Override
    public int hashCode() {
        return inetAddress.hashCode();
    }
}
