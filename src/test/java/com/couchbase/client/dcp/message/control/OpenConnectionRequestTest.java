package com.couchbase.client.dcp.message.control;

import com.couchbase.client.dcp.message.internal.OpenConnectionRequest;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OpenConnectionRequestTest {

    @Test
    public void testInit() {
        ByteBuf buffer = Unpooled.buffer();
        assertFalse(OpenConnectionRequest.is(buffer));

        OpenConnectionRequest.init(buffer);

        assertEquals(24, buffer.writerIndex());
        assertTrue(OpenConnectionRequest.is(buffer));
    }

    @Test
    public void testSetConnectionName() {
        ByteBuf buffer = Unpooled.buffer();
        OpenConnectionRequest.init(buffer);

        ByteBuf name = Unpooled.copiedBuffer("name", CharsetUtil.UTF_8);
        OpenConnectionRequest.connectionName(buffer, name);

        assertEquals("name", OpenConnectionRequest.connectionName(buffer).toString(CharsetUtil.UTF_8));
    }

}