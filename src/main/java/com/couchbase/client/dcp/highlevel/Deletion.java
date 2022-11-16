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

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.dcp.highlevel.internal.CollectionsManifest;
import com.couchbase.client.dcp.highlevel.internal.FlowControlReceipt;

public class Deletion extends DocumentChange {
  private final boolean dueToExpiration;

  public Deletion(ByteBuf byteBuf, CollectionsManifest.CollectionInfo collectionInfo, String key, FlowControlReceipt receipt, StreamOffset offset, boolean dueToExpiration) {
    super(byteBuf, collectionInfo, key, receipt, offset);

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
