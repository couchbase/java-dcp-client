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

package examples;

import com.couchbase.client.dcp.state.PartitionState;
import com.couchbase.client.dcp.state.SessionState;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import rx.functions.Action1;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class CheckSessionState {
  private static final ObjectMapper JACKSON = new ObjectMapper();

  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println("At least one path to serialized session required.");
      System.exit(1);
    }
    for (String sessionStatePath : args) {
      try {
        File sessionStateFile = new File(sessionStatePath);
        SessionState sessionState = JACKSON.readValue(sessionStateFile, SessionState.class);
        final AtomicInteger idx = new AtomicInteger(0);
        sessionState.foreachPartition(new Action1<PartitionState>() {
          @Override
          public void call(PartitionState state) {
            int partition = idx.getAndIncrement();
            if (lessThan(state.getEndSeqno(), state.getStartSeqno())) {
              System.out.printf("stream request for partition %d will fail because " +
                      "start sequence number (%d) is larger than " +
                      "end sequence number (%d)\n",
                  partition, state.getStartSeqno(), state.getEndSeqno());
            }
            if (lessThan(state.getStartSeqno(), state.getSnapshotStartSeqno())) {
              System.out.printf("stream request for partition %d will fail because " +
                      "snapshot start sequence number (%d) must not be larger than " +
                      "start sequence number (%d)\n",
                  partition, state.getSnapshotStartSeqno(), state.getStartSeqno());
            }
            if (lessThan(state.getSnapshotEndSeqno(), state.getStartSeqno())) {
              System.out.printf("stream request for partition %d will fail because " +
                      "start sequence number (%d) must not be larger than " +
                      "snapshot end sequence number (%d)\n",
                  partition, state.getStartSeqno(), state.getSnapshotEndSeqno());
            }
          }
        });
      } catch (IOException e) {
        System.out.println("Failed to decode " + sessionStatePath + ": " + e);
      }
    }
  }

  /**
   * @return true if x < y, where x and y treated as unsigned long int
   */
  private static boolean lessThan(long x, long y) {
    return (x < y) ^ (x < 0) ^ (y < 0);
  }
}
