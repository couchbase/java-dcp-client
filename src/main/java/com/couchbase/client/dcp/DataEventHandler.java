package com.couchbase.client.dcp;


import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public interface DataEventHandler {

    void onEvent(ByteBuf event);

}
