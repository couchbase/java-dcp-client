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

package com.couchbase.client.dcp.test;

import com.couchbase.client.dcp.test.agent.DcpStreamer;
import com.couchbase.client.dcp.test.agent.StreamerService;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class RemoteDcpStreamer implements Closeable {
  private final StreamerService streamerService;
  private final String streamerId;
  private static final long DEFAULT_TIMEOUT_SECONDS = 60;
  private static final long DEFAULT_QUIET_PERIOD_SECONDS = 3;

  public RemoteDcpStreamer(StreamerService streamerService, String streamerId) {
    this.streamerService = requireNonNull(streamerService);
    this.streamerId = requireNonNull(streamerId);
  }

  public RemoteDcpStreamer(RemoteAgent agent, String streamerId) {
    this(agent.streamer(), streamerId);
  }

  @Override
  public void close() {
    streamerService.stop(streamerId);
  }

  /**
   * Throws {@link AssertionError} if this stream's observed mutation count does not reach the expected value
   * before {@link #DEFAULT_TIMEOUT_SECONDS} elapse, or if additional mutations are observed within the subsequent
   * {@link #DEFAULT_QUIET_PERIOD_SECONDS}.
   */
  public void assertMutationCount(int expectedMutationCount) {
    assertMutationCount(expectedMutationCount, DEFAULT_TIMEOUT_SECONDS, SECONDS);
  }

  /**
   * Throws {@link AssertionError} if this stream's observed mutation count does not reach the expected value
   * within the given timeout period, or if additional mutations are observed within the subsequent
   * {@link #DEFAULT_QUIET_PERIOD_SECONDS}.
   */
  public void assertMutationCount(int expectedMutationCount,
                                  long timeout, TimeUnit timeoutUnit) {
    assertMutationCount(expectedMutationCount, timeout, timeoutUnit, DEFAULT_QUIET_PERIOD_SECONDS, SECONDS);
  }

  /**
   * Throws {@link AssertionError} if this stream's observed mutation count does not reach the expected value
   * within the given timeout period, or if additional mutations are observed within the subsequent
   * given quiet period.
   */
  public void assertMutationCount(int expectedMutationCount,
                                  long timeout, TimeUnit timeoutUnit,
                                  long quietPeriod, TimeUnit quietPeriodUnit) {
    DcpStreamer.Status status;

    // wait until expected mutations are observed
    status = streamerService.awaitMutationCount(streamerId, expectedMutationCount, timeout, timeoutUnit);
    assertEquals(expectedMutationCount, status.getMutations());

    // wait a bit longer to make sure no more arrive
    status = streamerService.awaitMutationCount(streamerId, expectedMutationCount + 1, quietPeriod, quietPeriodUnit);
    assertEquals(expectedMutationCount, status.getMutations());
  }

  public DcpStreamer.Status awaitStreamEnd() throws TimeoutException {
    return awaitStreamEnd(DEFAULT_TIMEOUT_SECONDS, SECONDS);
  }

  private DcpStreamer.Status awaitStreamEnd(long timeout, TimeUnit timeoutUnit) throws TimeoutException {
    return streamerService.awaitStreamEnd(streamerId, timeout, timeoutUnit);
  }

  public DcpStreamer.Status status() {
    return streamerService.status(streamerId);
  }
}
