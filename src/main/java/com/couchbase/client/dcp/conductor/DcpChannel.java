package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.message.control.ControlEvent;
import com.couchbase.client.dcp.message.internal.DcpOpenStreamRequest;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetAddress;

/**
 * Logical representation of a DCP cluster connection.
 *
 * The equals and hashcode are based on the {@link InetAddress}.
 */
public class DcpChannel {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpChannel.class);

    private final ClientEnvironment env;
    private final InetAddress inetAddress;
    private final Subject<ControlEvent, ControlEvent> controlEvents;
    private volatile Channel channel;

    public DcpChannel(InetAddress inetAddress, ClientEnvironment env) {
        this.inetAddress = inetAddress;
        this.env = env;
        this.controlEvents = PublishSubject.<ControlEvent>create().toSerialized();
    }

    public void connect() {
        final Bootstrap bootstrap = new Bootstrap()
            .remoteAddress(inetAddress, 11210)
            .channel(ChannelUtils.channelForEventLoopGroup(env.eventLoopGroup()))
            .handler(new DcpPipeline(env, controlEvents))
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


    public Completable openStream(final short vbid, long vbuuid, long startSeqno, long endSeqno,
        long snapshotStartSeqno, long snapshotEndSeqno) {

        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(Completable.CompletableSubscriber subscriber) {
                ByteBuf buffer = Unpooled.buffer();
                DcpOpenStreamRequest.init(buffer, vbid);
                channel.writeAndFlush(buffer);

              //  controlEvents.
            }
        });



        // write and listen on control events, "clear" it when this one comes up and compelte the
        // completable.
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
