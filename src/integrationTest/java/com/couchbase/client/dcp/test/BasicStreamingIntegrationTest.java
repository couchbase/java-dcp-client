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
import com.couchbase.client.dcp.test.agent.DcpStreamer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BasicStreamingIntegrationTest extends DcpIntegrationTestBase {
  @Test
  void canStreamFromBeginningToNow() throws Exception {
    couchbase().loadSampleBucket("beer-sample");

    try (RemoteDcpStreamer streamer = newStreamer("beer-sample")
        .range(StreamFrom.BEGINNING, StreamTo.NOW)
        .start()) {

      assertStatus(streamer.awaitStreamEnd(), 7303, 0, 0);
    }
  }

  @Test
  void canStreamFromNowToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.NOW, StreamTo.INFINITY)
          .start()) {

        streamer.assertStateCount(0, DcpStreamer.State.MUTATIONS);
        bucket.createOneDocumentInEachVbucket("b");
        streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  void canStreamFromBeginningToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
        streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);
        bucket.createOneDocumentInEachVbucket("b");
        streamer.assertStateCount(batchSize * 2, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  void rollbackMitigationWillBufferUnpersistedEvents() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .start()) {

        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

        // This assertion also acts as a barrier that ensures all documents from
        // the first batch are observed before persistence is stopped.
        streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);

        bucket.stopPersistence();
        bucket.createOneDocumentInEachVbucket("b");

        // expect to see all of "a" and none of "b"
        streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);

        bucket.startPersistence();

        // Now wait and expect both batches
        streamer.assertStateCount(batchSize * 2, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  void streamerRecoversFromRollbacksWithoutPersistencePolling() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          // DO NOT mitigate rollbacks
          .start()) {

        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();
        streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);

        bucket.stopPersistence();
        bucket.createOneDocumentInEachVbucket("b");

        // Since persistence polling is disabled, expect all of "a" and all of "b"
        streamer.assertStateCount(batchSize * 2, DcpStreamer.State.MUTATIONS);

        // Force a reconnect. Client will resume streaming will resume from the
        // unpersisted state, which triggers a rollback.
        couchbase().killMemcached(); // implicitly starts persistence
        couchbase().waitForReadyState();

        bucket.createOneDocumentInEachVbucket("c");

        // Streamer doesn't remove documents after a rollback, so we should see
        // all three batches.
        streamer.assertStateCount(batchSize * 3, DcpStreamer.State.MUTATIONS);

        // Make sure each partition rolled back once, and the rollbacks
        // didn't cause stream failures.
        streamer.assertStateCount(0, DcpStreamer.State.STREAM_FAILURES);
        streamer.assertStateCount(batchSize, DcpStreamer.State.ROLLBACKS);
      }
    }
  }

  @Test
  void rollbackMitigationClearsEventBufferOnReconnect() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .start()) {

        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();
        streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);

        bucket.stopPersistence();
        bucket.createOneDocumentInEachVbucket("b");

        // expect to see all of "a" and none of "b"
        streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);

        // Discard unpersisted items and force a reconnect.
        // Streaming will resume from last observed persisted state.
        couchbase().killMemcached(); // implicitly starts persistence
        couchbase().waitForReadyState();

        bucket.createOneDocumentInEachVbucket("c");

        // Expect batches "a" and "c" ("b" was never persisted)
        streamer.assertStateCount(batchSize * 2, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  void clientReconnectsAfterServerRestart() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();
        agent().resetCluster();
        couchbase().restart();
        bucket.createOneDocumentInEachVbucket("b");
        streamer.assertStateCount(batchSize * 2, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  /**
   * For some time after a bucket is created, the bucket config reported by the server has an empty
   * partition map ("vBucketMap":[]) and CouchbaseBucketConfig.numberOfPartitions() returns zero.
   * When the client connects, make sure it waits for a non-empty partition map.
   */
  @Test
  void connectWaitsForPartitionMap() throws Exception {
    for (int i = 0; i < 3; i++) {
      try (TestBucket bucket = newBucket().createWithoutWaiting()) {
        assertEquals(1024, agent().streamer().getNumberOfPartitions(bucket.name()));
      }
    }
  }
}
