/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.buffer;

import com.couchbase.client.dcp.message.MessageUtil;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

class ObserveSeqnoResponse {
    private final boolean didFailover;
    private final short vbid;
    private final long vbuuid;
    private final long persistSeqno;
    private final long currentSeqno;
    private final long oldVbuuid;
    private final long lastSeqno;

    ObserveSeqnoResponse(final ByteBuf response) {
        ByteBuf content = MessageUtil.getContent(response);
        this.didFailover = content.readBoolean();
        this.vbid = content.readShort();
        this.vbuuid = content.readLong();
        this.persistSeqno = content.readLong();
        this.currentSeqno = content.readLong();
        this.oldVbuuid = this.didFailover ? content.readLong() : 0;
        this.lastSeqno = this.didFailover ? content.readLong() : 0;
    }

    public boolean didFailover() {
        return didFailover;
    }

    public short vbid() {
        return vbid;
    }

    public long vbuuid() {
        return vbuuid;
    }

    public long persistSeqno() {
        return persistSeqno;
    }

    public long currentSeqno() {
        return currentSeqno;
    }

    public long oldVbuuid() {
        return oldVbuuid;
    }

    public long lastSeqno() {
        return lastSeqno;
    }

    @Override
    public String toString() {
        return "ObserveSeqnoResponse{" +
                "didFailover=" + didFailover +
                ", vbid=" + vbid +
                ", vbuuid=" + vbuuid +
                ", persistSeqno=" + persistSeqno +
                ", currentSeqno=" + currentSeqno +
                ", oldVbuuid=" + oldVbuuid +
                ", lastSeqno=" + lastSeqno +
                '}';
    }
}
