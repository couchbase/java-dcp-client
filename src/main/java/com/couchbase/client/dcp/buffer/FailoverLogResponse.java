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
import com.couchbase.client.dcp.state.FailoverLogEntry;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class FailoverLogResponse {
    private final List<FailoverLogEntry> logEntries;

    public FailoverLogResponse(final ByteBuf response) {
        final ByteBuf content = MessageUtil.getContent(response);
        final int numEntries = content.readableBytes() / 16;
        final List<FailoverLogEntry> entries = new ArrayList<>(numEntries);

        for (int i = 0; i < numEntries; i++) {
            final long vbuuid = content.readLong();
            final long seqno = content.readLong();
            entries.add(new FailoverLogEntry(seqno, vbuuid));
        }

        this.logEntries = Collections.unmodifiableList(entries);
    }

    public List<FailoverLogEntry> getFailoverLogEntries() {
        return logEntries;
    }

    public long getCurrentVbuuid() {
        return logEntries.get(0).getUuid();
    }

    @Override
    public String toString() {
        return logEntries.toString();
    }
}
