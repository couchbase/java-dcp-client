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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class ScopesIntegrationTest extends DcpIntegrationTestBase {
  private static final int VB = 1024;
  private static final int NUMBER_OF_SCOPES = 1;

  @BeforeEach
  public void checkIsCheshireCat() {
    Version version = couchbase().getVersion().orElseThrow(() -> new RuntimeException("Missing Couchbase version"));
    assumeTrue(version.isAtLeast(new Version(7, 0, 0)));
  }

  @Test
  void scopesStreamFromBeginningToNow() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      int scopes = bucket.createScopes(NUMBER_OF_SCOPES, "S").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.BEGINNING, StreamTo.NOW)
          .collectionAware()
          .start()) {
        assertStatus(streamer.awaitStreamEnd(), 0, 0, 0, scopes * VB, 0, 0, 0, 0);
      }
    }

  }

  @Test
  void scopesStreamFromNowToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      int scopesA = bucket.createScopes(NUMBER_OF_SCOPES, "A").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.NOW, StreamTo.INFINITY)
          .collectionAware()
          .start()) {
        streamer.assertStateCount(0, DcpStreamer.State.SCOPE_CREATIONS);

        int scopesB = bucket.createScopes(NUMBER_OF_SCOPES, "B").size();
        //See only B scopes
        streamer.assertStateCount(scopesB * VB, DcpStreamer.State.SCOPE_CREATIONS);
      }
    }
  }

  @Test
  void scopesStreamFromBeginningToInfinity() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      int scopesA = bucket.createScopes(NUMBER_OF_SCOPES, "A").size();

      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.BEGINNING, StreamTo.INFINITY)
          .collectionAware()
          .start()) {
        streamer.assertStateCount(scopesA * VB, DcpStreamer.State.SCOPE_CREATIONS);

        int scopesB = bucket.createScopes(1, "B").size();
        //See both A and B scopes
        streamer.assertStateCount((scopesA + scopesB) * VB, DcpStreamer.State.SCOPE_CREATIONS);
      }
    }
  }

  @Test
  void scopesReconnectsAfterServerRestart() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .range(StreamFrom.BEGINNING, StreamTo.INFINITY)
          .collectionAware()
          .start()) {
        int scopesA = bucket.createScopes(NUMBER_OF_SCOPES, "A").size();

        streamer.assertStateCount(scopesA * VB, DcpStreamer.State.SCOPE_CREATIONS);

        agent().resetCluster();
        couchbase().restart();

        int scopesB = bucket.createScopes(NUMBER_OF_SCOPES, "B").size();
        //See both A and B scopes
        streamer.assertStateCount((scopesA + scopesB) * VB, DcpStreamer.State.SCOPE_CREATIONS);
      }
    }
  }

  @Disabled("Invalid -- rollback mitigation has no effect on scope creation notifications")
  @Test
  void scopesRollbackMitigationWillBufferUnpersistedEvents() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .collectionAware()
          .start()) {
        int scopesA = bucket.createScopes(NUMBER_OF_SCOPES, "A").size();

        streamer.assertStateCount(scopesA * VB, DcpStreamer.State.SCOPE_CREATIONS);

        bucket.stopPersistence();
        int scopesB = bucket.createScopes(NUMBER_OF_SCOPES, "B").size();
        //All of A non of B
        streamer.assertStateCount(scopesA * VB, DcpStreamer.State.SCOPE_CREATIONS);

        bucket.startPersistence();

        //See both A and B scopes
        streamer.assertStateCount((scopesA + scopesB) * VB, DcpStreamer.State.SCOPE_CREATIONS);
      }
    }
  }

  @Disabled("Invalid -- rollback mitigation has no effect on scope creation notifications")
  @Test
  void scopesRollbackMitigationClearsEventBufferOnReconnect() throws Exception {
    try (TestBucket bucket = newBucket().create()) {
      try (RemoteDcpStreamer streamer = bucket.newStreamer()
          .mitigateRollbacks()
          .collectionAware()
          .start()) {
        int scopesA = bucket.createScopes(NUMBER_OF_SCOPES, "A").size();
        streamer.assertStateCount(scopesA * VB, DcpStreamer.State.SCOPE_CREATIONS);

        bucket.stopPersistence();
        int scopesB = bucket.createScopes(NUMBER_OF_SCOPES, "B").size();
        //All of A non of B
        streamer.assertStateCount(scopesA * VB, DcpStreamer.State.SCOPE_CREATIONS);

        // Discard unpersisted items and force a reconnect.
        // Streaming will resume from last observed persisted state.
        couchbase().killMemcached(); // implicitly starts persistence
        couchbase().waitForReadyState();

        int scopesC = bucket.createScopes(NUMBER_OF_SCOPES, "C").size();

        //See both A and C scopes ( B never persisted)
        streamer.assertStateCount((scopesA + scopesC) * VB, DcpStreamer.State.SCOPE_CREATIONS);
      }
    }
  }
}
