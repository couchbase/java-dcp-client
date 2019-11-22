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

package com.couchbase.client.dcp.highlevel.internal;

import com.couchbase.client.dcp.highlevel.DatabaseChangeListener;

public interface DatabaseChangeEvent {
  /**
   * Pass this event to the appropriate method of the given listener.
   * <p>
   * The listener is invoked immediately in the same thread that calls this method.
   */
  void dispatch(DatabaseChangeListener listener);

  /**
   * Returns the id of the virtual bucket associated with this event,
   * or -1 if the event is not associated with a specific virtual bucket.
   */
  int getVbucket();
}
