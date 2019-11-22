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
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class Deletion extends DocumentChange {
  private final boolean dueToExpiration;

  public Deletion(ByteBuf byteBuf, FlowControlReceipt receipt, long vbucketUuid, SnapshotMarker snapshot, boolean dueToExpiration) {
    super(byteBuf, receipt, vbucketUuid, snapshot);

    this.dueToExpiration = dueToExpiration;
  }

  public boolean isDueToExpiration() {
    return dueToExpiration;
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onDeletion(this);
  }
}
