package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.message.DcpOpenStreamRequest;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.transport.netty.ChannelUtils;
import com.couchbase.client.dcp.transport.netty.DcpPipeline;
import com.couchbase.client.deps.io.netty.bootstrap.Bootstrap;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelFuture;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.functions.Action1;
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
    private final Subject<ByteBuf, ByteBuf> controlEvents;
    private volatile Channel channel;

    public DcpChannel(InetAddress inetAddress, ClientEnvironment env) {
        this.inetAddress = inetAddress;
        this.env = env;
        this.controlEvents = PublishSubject.<ByteBuf>create().toSerialized();

        this.controlEvents.subscribe(new Action1<ByteBuf>() {
            @Override
            public void call(ByteBuf buf) {
                //System.err.println("Got Control Message");
                //System.out.println(MessageUtil.humanize(buf));
            }
        });
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


    public Completable openStream(final short vbid, final long vbuuid, final long startSeqno, final long endSeqno,
                                  final long snapshotStartSeqno, final long snapshotEndSeqno) {
        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(Completable.CompletableSubscriber subscriber) {
                LOGGER.debug("Opening Stream against {} with vbid: {}, vbuuid: {}, startSeqno: {}, " +
                    "endSeqno: {},  snapshotStartSeqno: {}, snapshotEndSeqno: {}",
                    channel.remoteAddress(), vbid, vbuuid, startSeqno, endSeqno, snapshotStartSeqno, snapshotEndSeqno);

                ByteBuf buffer = Unpooled.buffer();
                DcpOpenStreamRequest.init(buffer, vbid);
                channel.writeAndFlush(buffer);

              //  controlEvents.
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
