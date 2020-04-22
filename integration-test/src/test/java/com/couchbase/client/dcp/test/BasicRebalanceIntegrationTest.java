package com.couchbase.client.dcp.test;

import com.couchbase.client.dcp.test.agent.DcpStreamer;
import org.junit.Test;

public class BasicRebalanceIntegrationTest extends DcpIntegrationTestBase {

  @Test
  public void rebalanceDoesNotDisruptStreaming() throws Exception {
    // Don't try to clean up the bucket, since the cluster will be broken
    // after the second node is stopped.
    final TestBucket bucket = newBucket().replicas(1).create();
    final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

    try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
      streamer.assertStateCount(batchSize, DcpStreamer.State.MUTATIONS);
      assertStatus(streamer.status(), batchSize, 0, 0);

      try (CouchbaseContainer secondNode = couchbase().addNode()) {
        couchbase().rebalance();
        bucket.createOneDocumentInEachVbucket("b");

        streamer.assertStateCount(batchSize * 2, DcpStreamer.State.MUTATIONS);
        assertStatus(streamer.status(), batchSize * 2, 0, 0);
      }
    }
  }
}
