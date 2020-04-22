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

public class CollectionsBasicStreamingIntegrationTest extends DcpIntegrationTestBase {

  private static final int VB = 1024;
  private static final int NUMBER_OF_SCOPES = 1;
  private static final int NUMBER_OF_COLLECTIONS = 1;

  @Before
  public void checkIsCheshireCat() {
    Version version = couchbase().getVersion().orElseThrow(() -> new RuntimeException("Missing Couchbase version"));
    Assume.assumeTrue(version.isAtLeast(new Version(7, 0, 0)));
  }

  @Test
  public void collectionsStreamFromBeginningToNow() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      int scopes = bucket.createScopes(NUMBER_OF_SCOPES, "S").size();
      int collections = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C", "S0").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .collectionAware()
          .range(StreamFrom.BEGINNING, StreamTo.NOW)
          .start()) {
        assertStatus(streamer.awaitStreamEnd(), 0, 0, 0, scopes * VB, 0, collections * VB, 0, 0);
      }
    }
  }

  @Test
  public void collectionsStreamFromNowToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      bucket.createScopes(NUMBER_OF_SCOPES, "S-a");
      bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-a", "S-a0");

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.NOW, StreamTo.INFINITY)
          .collectionAware()
          .start()) {
        streamer.assertStateCount(0, DcpStreamer.State.SCOPE_CREATIONS);
        streamer.assertStateCount(0, DcpStreamer.State.COLLECTION_CREATIONS);

        int scopesB = bucket.createScopes(NUMBER_OF_SCOPES, "S-b").size();
        int collectionsB = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-b", "S-b0").size();

        streamer.assertStateCount(scopesB * VB, DcpStreamer.State.SCOPE_CREATIONS);
        streamer.assertStateCount(collectionsB * VB, DcpStreamer.State.COLLECTION_CREATIONS);
      }
    }
  }

  @Test
  public void collectionsStreamFromBeginningToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      int scopesA = bucket.createScopes(NUMBER_OF_SCOPES, "S-a").size();
      int collectionsA = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-a", "S-a0").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .collectionAware()
          .range(StreamFrom.BEGINNING, StreamTo.INFINITY)
          .start()) {
        streamer.assertStateCount(scopesA * VB, DcpStreamer.State.SCOPE_CREATIONS);
        streamer.assertStateCount(collectionsA * VB, DcpStreamer.State.COLLECTION_CREATIONS);
      }
    }
  }

  @Test
  public void collectionsClientReconnectsAfterServerRestart() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .collectionAware()
          .start()) {
        int scopesA = bucket.createScopes(NUMBER_OF_SCOPES, "S-a").size();
        int collectionsA = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-a", "S-a0").size();

        agent().resetCluster();
        couchbase().restart();

        int scopesB = bucket.createScopes(NUMBER_OF_SCOPES, "S-b").size();
        int collectionsB = bucket.createCollections(NUMBER_OF_COLLECTIONS, "C-b", "S-b0").size();

        streamer.assertStateCount((scopesA + scopesB) * VB, DcpStreamer.State.SCOPE_CREATIONS);
        streamer.assertStateCount((collectionsA + collectionsB) * VB, DcpStreamer.State.COLLECTION_CREATIONS);
      }
    }
  }

}
