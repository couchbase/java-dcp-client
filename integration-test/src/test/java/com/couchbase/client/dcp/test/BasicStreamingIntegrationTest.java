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

import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BasicStreamingIntegrationTest extends DcpIntegrationTestBase {
  @Test
  public void canStreamFromBeginningToNow() throws Exception {
    couchbase().loadSampleBucket("beer-sample");

    try (RemoteDcpStreamer streamer = newStreamer("beer-sample")
        .range(StreamFrom.BEGINNING, StreamTo.NOW)
        .start()) {

      assertStatus(streamer.awaitStreamEnd(), 7303, 0, 0);
    }
  }

  @Test
  public void canStreamFromNowToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.NOW, StreamTo.INFINITY)
          .start()) {

        streamer.assertMutationCount(0);
        bucket.createOneDocumentInEachVbucket("b").size();
        streamer.assertMutationCount(batchSize);
      }
    }
  }

  @Test
  public void canStreamFromBeginningToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
        streamer.assertMutationCount(batchSize);
        bucket.createOneDocumentInEachVbucket("b");
        streamer.assertMutationCount(batchSize * 2);
      }
    }
  }

  @Test
  public void rollbackMitigationWillBufferUnpersistedEvents() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .start()) {

        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();
        bucket.stopPersistence();
        bucket.createOneDocumentInEachVbucket("b");

        // expect to see all of "a" and none of "b"
        streamer.assertMutationCount(batchSize);

        bucket.startPersistence();

        // Now wait and expect both batches
        streamer.assertMutationCount(batchSize * 2);
      }
    }
  }

  @Test
  public void rollbackMitigationClearsEventBufferOnReconnect() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .start()) {

        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();
        streamer.assertMutationCount(batchSize);

        bucket.stopPersistence();
        bucket.createOneDocumentInEachVbucket("b");

        // expect to see all of "a" and none of "b"
        streamer.assertMutationCount(batchSize);

        // Discard unpersisted items and force a reconnect.
        // Streaming will resume from last observed persisted state.
        couchbase().killMemcached(); // implicitly starts persistence
        couchbase().waitForReadyState();

        bucket.createOneDocumentInEachVbucket("c");

        // Expect batches "a" and "c" ("b" was never persisted)
        streamer.assertMutationCount(batchSize * 2);
      }
    }
  }

  @Test
  public void clientReconnectsAfterServerRestart() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();
        couchbase().restart();
        bucket.createOneDocumentInEachVbucket("b").size();
        streamer.assertMutationCount(batchSize * 2);
      }
    }
  }

  /**
   * For some time after a bucket is created, the bucket config reported by the server has an empty
   * partition map ("vBucketMap":[]) and CouchbaseBucketConfig.numberOfPartitions() returns zero.
   * When the client connects, make sure it waits for a non-empty partition map.
   */
  @Test
  public void connectWaitsForPartitionMap() throws Exception {
    for (int i = 0; i < 3; i++) {
      try (TestBucket bucket = newBucket().createWithoutWaiting()) {
        assertEquals(1024, agent().streamer().getNumberOfPartitions(bucket.name()));
      }
    }
  }
}
