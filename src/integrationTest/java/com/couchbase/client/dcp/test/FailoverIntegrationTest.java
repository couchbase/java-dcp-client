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
import com.couchbase.testcontainers.custom.CouchbaseContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FailoverIntegrationTest extends DcpIntegrationTestBase {

  private static CouchbaseContainer secondNode;

  @BeforeAll
  public static void startSecondNode() {
    secondNode = couchbase().addNode();
    couchbase().rebalance();
  }

  @AfterAll
  public static void stopSecondNode() {
    stop(secondNode);
  }

  @Test
  void failover() throws Exception {
    final TestBucket bucket = newBucket().replicas(1).create();

    final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

    try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
      streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);

      secondNode.failover();
      couchbase().rebalance();

      bucket.createOneDocumentInEachVbucket("b");
      streamer.assertStateCount(batchSize * 2, DcpStreamer.State.MUTATIONS);
    }
  }
}
