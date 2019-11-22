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

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.message.StreamEndReason;

public interface DatabaseChangeListener {

  /**
   * Called when a Couchbase document is created or updated.
   * <p>
   * <b>NOTE:</b> If flow control is enabled on the client, listeners registered via
   * {@link Client#nonBlockingListener(DatabaseChangeListener)} and listeners using
   * {@link FlowControlMode#MANUAL} <b>MUST</b> call {@link Mutation#flowControlAck()}
   * when the application has finished processing the event, otherwise the server will
   * stop sending events.
   */
  default void onMutation(Mutation mutation) {
    mutation.flowControlAck();
  }

  /**
   * Called when a Couchbase document is deleted or expired.
   * <p>
   * <b>NOTE:</b> If flow control is enabled on the client, listeners registered via
   * {@link Client#nonBlockingListener(DatabaseChangeListener)} and listeners using
   * {@link FlowControlMode#MANUAL} <b>MUST</b> call {@link Deletion#flowControlAck()}
   * when the application has finished processing the event, otherwise the server will
   * stop sending events.
   */
  default void onDeletion(Deletion deletion) {
    deletion.flowControlAck();
  }

  default void onRollback(Rollback rollback) {
    // Most clients just want to resume streaming.
    rollback.resume();
  }

  default void onSnapshot(SnapshotDetails snapshotDetails) {
    // Most clients won't care, since snapshot markers are available
    // in the "offset" of mutations and deletions.
  }

  default void onFailoverLog(FailoverLog failoverLog) {
    // Most clients won't care.
  }

  /**
   * <b>NOTE:</b> The DCP client will automatically attempt to reopen the stream
   * if the reason is not {@link StreamEndReason#OK}.
   */
  default void onStreamEnd(StreamEnd streamEnd) {
    // Most clients won't care, since the client auto-reopens aborted streams.
  }

  /**
   * Something bad and probably unrecoverable happened.
   */
  void onFailure(StreamFailure streamFailure);

}
