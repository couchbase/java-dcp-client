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

package com.couchbase.client.dcp.test;

import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.test.agent.DcpStreamer;
import com.couchbase.client.dcp.util.Version;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CollectionsMutationsIntegrationTest extends DcpIntegrationTestBase {

  private static final int NUMBER_OF_SCOPES = 2;
  private static final int NUMBER_OF_COLLECTIONS = 5;

  @Before
  public void checkIsCheshireCat() {
    Version version = couchbase().getVersion().orElseThrow(() -> new RuntimeException("Missing Couchbase version"));
    Assume.assumeTrue(version.isAtLeast(new Version(7, 0, 0)));
  }

  @Test
  public void canStreamFromBeginningToNow() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      bucket.createScopes(NUMBER_OF_SCOPES, "S").size();
      List<String> collections = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C", "S0");
      Thread.sleep(1000);
      int documentsSize = bucket.upsertOneDocumentToEachCollection(collections, "D", "S0").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.BEGINNING, StreamTo.NOW)
          .collectionAware()
          .start()) {

        //Expect to see all events
        assertStatus(streamer.awaitStreamEnd(), documentsSize, 0, 0);
      }
    }


  }

  @Test
  public void canStreamFromNowToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      bucket.createScopes(NUMBER_OF_SCOPES, "S");
      List<String> collections = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C", "S0");
      Thread.sleep(1000);
      bucket.upsertOneDocumentToEachCollection(collections, "D-a", "S0").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.NOW, StreamTo.INFINITY)
          .collectionAware()
          .start()) {
        //Expect to not see any events
        streamer.assertStateCount(0, DcpStreamer.State.SCOPE_CREATIONS);
        streamer.assertStateCount(0, DcpStreamer.State.COLLECTION_CREATIONS);
        streamer.assertStateCount(0, DcpStreamer.State.MUTATIONS);

        int documentsSize = bucket.upsertOneDocumentToEachCollection(collections, "D-b", "S0").size();
        //See only new mutations
        streamer.assertStateCount(documentsSize, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  public void canStreamFromBeginningToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      bucket.createScopes(NUMBER_OF_SCOPES, "S");
      List<String> collections = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-a", "S0");
      Thread.sleep(1000);
      int documentsSize = bucket.upsertOneDocumentToEachCollection(collections, "D-a", "S0").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .collectionAware()
          .start()) {
        //Expect to see all previous events
        streamer.assertStateCount(documentsSize, DcpStreamer.State.MUTATIONS);

        List<String> collectionsB = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-b", "S0");
        bucket.upsertOneDocumentToEachCollection(collectionsB, "D-b", "S0");

        //See old and new events
        streamer.assertStateCount(documentsSize * 2, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  public void rollbackMitigationWillBufferUnpersistedEvents() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .collectionAware()
          .start()) {
        //Create scopes, create collections on scope 'S0', add 1 document each to collections
        bucket.createScopes(NUMBER_OF_SCOPES, "S");
        List<String> collections = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-a", "S0");
        Thread.sleep(1000);
        int documentsSizeA = bucket.upsertOneDocumentToEachCollection(collections, "D-a", "S0").size();

        // This assertion also acts as a barrier that ensures all documents from
        // the first batch are observed before persistence is stopped.
        streamer.assertStateCount(documentsSizeA, DcpStreamer.State.MUTATIONS);

        bucket.stopPersistence();
        //Add 1 document to collections again
        int documentsSizeB = bucket.upsertOneDocumentToEachCollection(collections, "D-b", "S0").size();

        // expect to see all of "D-a" and none of "D-b"
        streamer.assertStateCount(documentsSizeA, DcpStreamer.State.MUTATIONS);

        bucket.startPersistence();

        // Now wait and expect both batches
        streamer.assertStateCount(documentsSizeA + documentsSizeB, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  public void rollbackMitigationClearsEventBufferOnReconnect() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .collectionAware()
          .start()) {
        bucket.createScopes(NUMBER_OF_SCOPES, "S");
        List<String> collections = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-a", "S0");
        Thread.sleep(1000);
        int documentsSizeA = bucket.upsertOneDocumentToEachCollection(collections, "D-a", "S0").size();

        streamer.assertStateCount(documentsSizeA, DcpStreamer.State.MUTATIONS);

        bucket.stopPersistence();
        bucket.upsertOneDocumentToEachCollection(collections, "D-b", "S0");

        // expect to see all of "D-a" and none of "D-b"
        streamer.assertStateCount(documentsSizeA, DcpStreamer.State.MUTATIONS);

        // Discard unpersisted items and force a reconnect.
        // Streaming will resume from last observed persisted state.
        couchbase().killMemcached(); // implicitly starts persistence
        couchbase().waitForReadyState();

        int documentsSizeC = bucket.upsertOneDocumentToEachCollection(collections, "D-c", "S0").size();

        // Expect batches "D-a" and "D-c" ("D-b" was never persisted)
        streamer.assertStateCount(documentsSizeA + documentsSizeC, DcpStreamer.State.MUTATIONS);
      }
    }
  }

  @Test
  public void clientReconnectsAfterServerRestart() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .collectionAware()
          .start()) {
        bucket.createScopes(NUMBER_OF_SCOPES, "S");
        List<String> collections = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-a", "S0");
        Thread.sleep(1000);
        int documentsSizeA = bucket.upsertOneDocumentToEachCollection(collections, "D-a", "S0").size();

        streamer.assertStateCount(documentsSizeA, DcpStreamer.State.MUTATIONS);

        agent().resetCluster();
        couchbase().restart();

        int documentsSizeB = bucket.upsertOneDocumentToEachCollection(collections, "D-b", "S0").size();
        streamer.assertStateCount(documentsSizeA + documentsSizeB, DcpStreamer.State.MUTATIONS);
      }
    }
  }
}
