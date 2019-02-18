/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;

public enum HelloRequest {
    ;

    public static final short DATATYPE = 0x01;
    public static final short TLS = 0x02;
    public static final short TCPNODELAY = 0x03;
    public static final short MUTATIONSEQ = 0x04;
    public static final short TCPDELAY = 0x05;
    public static final short XATTR = 0x06;
    public static final short XERROR = 0x07;
    public static final short SELECT = 0x08;

    /**
     * Enable snappy-based compression support.
     *
     * @since Couchbase Server 5.5 (Vulcan)
     */
    public static final short SNAPPY = 0x0a;

    private static final short[] standardFeatures = new short[]{XERROR, SELECT};

    public static void init(ByteBuf buffer, String connectionName, short... extraFeatures) {
        MessageUtil.initRequest(MessageUtil.HELLO_OPCODE, buffer);
        MessageUtil.setKey(connectionName, buffer);

        ByteBuf features = Unpooled.buffer((standardFeatures.length + extraFeatures.length) * 2);
        try {
            for (short feature : standardFeatures) {
                features.writeShort(feature);
            }
            for (short feature : extraFeatures) {
                features.writeShort(feature);
            }

            MessageUtil.setContent(features, buffer);

        } finally {
            features.release();
        }
    }
}
