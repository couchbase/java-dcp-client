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
package com.couchbase.client.dcp.message;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import static com.couchbase.client.dcp.message.MessageUtil.DCP_MUTATION_OPCODE;
import static com.couchbase.client.dcp.message.MessageUtil.DCP_SNAPSHOT_MARKER_OPCODE;

public enum DcpSnapshotMarkerMessage {
    ;

    public static boolean is(final ByteBuf buffer) {
        return buffer.getByte(0) == MessageUtil.MAGIC_REQ && buffer.getByte(1) == DCP_SNAPSHOT_MARKER_OPCODE;
    }

    public static SnapshotType type(final ByteBuf buffer) {
        int type = MessageUtil.getExtras(buffer).getInt(16);
        switch (type) {
            case 0x00: return SnapshotType.MEMORY;
            case 0x02: return SnapshotType.DISK;
            case 0x04: return SnapshotType.CHECKPOINT;
            case 0x08: return SnapshotType.ACK;
            default: throw new IllegalStateException("Unknown Snapshot Type: " + type);
        }
    }

    public static long startSeqno(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getLong(0);

    }

    public static long endSeqno(final ByteBuf buffer) {
        return MessageUtil.getExtras(buffer).getLong(8);
    }

    public static String toString(final ByteBuf buffer) {
        return "SnapshotMarker [vbid: " + partition(buffer) + ", type: " + type(buffer) + ", start: " + startSeqno(buffer) + ", end: "
            +  endSeqno(buffer) + "]";
    }

    public static short partition(final ByteBuf buffer) {
        return MessageUtil.getVbucket(buffer);
    }

    public enum SnapshotType {
        /**
         * Specifies that the snapshot contains in-meory items only.
         */
        MEMORY,
        /**
         * Specifies that the snapshot contains on-disk items only.
         */
        DISK,
        /**
         * An internally used flag for intra-cluster replication to help to keep in-memory datastructures look similar.
         */
        CHECKPOINT,
        /**
         * Specifies that this snapshot marker should return a response once the entire snapshot is received.
         */
        ACK
    }


}
