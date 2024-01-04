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

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.test.agent.DcpStreamer.Status;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface StreamerService {

  List<Integer> ALL_VBUCKETS = Collections.emptyList();

  /**
   * @param bucket Name of the bucket to stream from.
   * @param vbuckets List of vBuckets to stream from, or empty list for all vBuckets.
   * @return ID of the new streamer.
   */
  String start(String bucket, List<Integer> vbuckets, StreamFrom from, StreamTo to, boolean mitigateRollbacks, boolean collectionAware);

  /**
   * Immediately disconnects the streamer.
   *
   * @param streamerId ID of the streamer to stop.
   */
  void stop(String streamerId);

  /**
   * @return IDs of all active streamers.
   */
  Set<String> list();

  /**
   * @param streamerId ID of the streamer to examine.
   * @return The streamer's session state as a JSON string.
   */
  String get(String streamerId);

  /**
   * Waits for the stream to reach the "stream to" value, then returns the streamer status
   * and stops the streamer.
   *
   * @throws TimeoutException if stream end is not reached before deadline
   * @throws IllegalStateException if "stream to" condition is "infinity"
   */
  Status awaitStreamEnd(String streamerId, long timeout, TimeUnit unit) throws TimeoutException;

  /**
   * Get the status of a streamer
   *
   * @return the status
   */
  Status status(String streamerId);

  /**
   * Creates a temporary DCP client, connects to the bucket, and returns the number of partitions from the bucket config.
   *
   * @param bucket Name of the bucket to query
   */
  int getNumberOfPartitions(String bucket);

  /**
   * Waits for the stream to reach the {@link DcpStreamer.State} count or for the timeout to expire (whichever comes first)
   * then returns the streamer status.
   */
  Status awaitStateCount(String streamerId, DcpStreamer.State state, int stateCount, long timeout, TimeUnit unit);

}
