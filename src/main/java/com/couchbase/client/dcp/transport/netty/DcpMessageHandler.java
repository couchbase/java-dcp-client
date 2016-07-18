package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.message.*;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.*;
import rx.subjects.Subject;

public class DcpMessageHandler extends ChannelDuplexHandler {

    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(DcpMessageHandler.class);

    private final DataEventHandler dataEventHandler;
    private final Subject<ByteBuf, ByteBuf> controlEvents;

    public DcpMessageHandler(DataEventHandler dataEventHandler, Subject<ByteBuf, ByteBuf> controlEvents) {
        this.dataEventHandler = dataEventHandler;
        this.controlEvents = controlEvents;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf message = (ByteBuf) msg;

        if (isDataMessage(message)) {
            dataEventHandler.onEvent(message);
        } else if (isControlMessage(message)) {
            controlEvents.onNext(message);
        } else {
            LOGGER.warn("Unknown DCP Message, ignoring. \n{}", MessageUtil.humanize(message));
        }
    }

    private static boolean isControlMessage(ByteBuf msg) {
        return DcpOpenStreamResponse.is(msg) || DcpStreamEndMessage.is(msg) || DcpSnapshotMarkerMessage.is(msg);
    }

    private static boolean isDataMessage(ByteBuf msg) {
        return DcpMutationMessage.is(msg) || DcpDeletionMessage.is(msg) || DcpExpirationMessage.is(msg);
    }

}
