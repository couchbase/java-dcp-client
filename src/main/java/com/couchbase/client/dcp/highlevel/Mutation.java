/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.dcp.highlevel;

import com.couchbase.client.dcp.highlevel.internal.FlowControlReceipt;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class Mutation extends DocumentChange {
  private final int flags;
  private final int expiry;
  private final int lockTime;

  public Mutation(ByteBuf byteBuf, FlowControlReceipt receipt, long vbucketUuid, SnapshotMarker snapshot) {
    super(byteBuf, receipt, vbucketUuid, snapshot);

    this.flags = DcpMutationMessage.flags(byteBuf);
    this.expiry = DcpMutationMessage.expiry(byteBuf);
    this.lockTime = DcpMutationMessage.lockTime(byteBuf);
  }

  public int getExpiry() {
    return expiry;
  }

  public int getLockTime() {
    return lockTime;
  }

  public int getFlagsAsInt() {
    return flags;
  }

  public boolean isJson() {
    final int commonFlags = flags >> 24;
    if (commonFlags == 0) {
      return flags == 0;
    }

    return (commonFlags & 0xf) == 2;
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onMutation(this);
  }
}
