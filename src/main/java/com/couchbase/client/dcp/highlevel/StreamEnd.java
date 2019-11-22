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

import com.couchbase.client.dcp.highlevel.internal.DatabaseChangeEvent;
import com.couchbase.client.dcp.message.StreamEndReason;

import static java.util.Objects.requireNonNull;

public class StreamEnd implements DatabaseChangeEvent {
  private final int vbucket;
  private final StreamEndReason reason;

  public StreamEnd(int vbucket, StreamEndReason reason) {
    this.vbucket = vbucket;
    this.reason = requireNonNull(reason);
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onStreamEnd(this);
  }

  @Override
  public int getVbucket() {
    return vbucket;
  }

  public StreamEndReason getReason() {
    return reason;
  }
}
