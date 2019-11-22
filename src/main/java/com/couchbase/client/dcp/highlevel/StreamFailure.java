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

import static java.util.Objects.requireNonNull;

public class StreamFailure implements DatabaseChangeEvent {
  private final int vbucket;
  private final Throwable throwable;

  public StreamFailure(int vbucket, Throwable throwable) {
    this.vbucket = vbucket;
    this.throwable = requireNonNull(throwable);
  }

  @Override
  public void dispatch(DatabaseChangeListener listener) {
    listener.onFailure(this);
  }

  public Throwable getCause() {
    return throwable;
  }

  /**
   * Returns the partition that experience the failure, or -1 if the failure is not specific to a partition.
   */
  @Override
  public int getVbucket() {
    return vbucket;
  }
}
