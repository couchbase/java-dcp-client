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
package com.couchbase.client.dcp.message.control;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.dcp.message.DcpOpenConnectionRequest;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpenConnectionRequestTest {

  @Test
  void testInit() {
    ByteBuf buffer = Unpooled.buffer();
    assertFalse(DcpOpenConnectionRequest.is(buffer));

    DcpOpenConnectionRequest.init(buffer, emptySet());

    assertEquals(32, buffer.writerIndex());
    assertTrue(DcpOpenConnectionRequest.is(buffer));
  }

  @Test
  void testSetConnectionName() {
    ByteBuf buffer = Unpooled.buffer();
    DcpOpenConnectionRequest.init(buffer, emptySet());

    DcpOpenConnectionRequest.connectionName(buffer, "name");

    assertEquals("name", DcpOpenConnectionRequest.connectionName(buffer).toString(UTF_8));
  }

}
