package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.dcp.message.control.ControlEvent;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.channel.*;
import rx.subjects.Subject;

public class DcpMessageHandler extends ChannelDuplexHandler {

    private final DataEventHandler dataEventHandler;
    private final Subject<ControlEvent, ControlEvent> controlEvents;

    public DcpMessageHandler(DataEventHandler dataEventHandler, Subject<ControlEvent, ControlEvent> controlEvents) {
        this.dataEventHandler = dataEventHandler;
        this.controlEvents = controlEvents;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        System.err.println(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
       ctx.write(msg, promise);
    }
}
