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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class FailoverIntegrationTest extends DcpIntegrationTestBase {

    private static CouchbaseContainer secondNode;

    @BeforeClass
    public static void startSecondNode() {
        secondNode = couchbase().addNode();
        couchbase().rebalance();
    }

    @AfterClass
    public static void stopSecondNode() {
        stop(secondNode);
    }

    @Test
    public void failover() throws Exception {
        final TestBucket bucket = newBucket().replicas(1).create();

        final int batchSize = bucket.createOneDocumentInEachVbucket("a").size();

        try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
            streamer.assertMutationCount(batchSize);

            secondNode.failover();
            couchbase().rebalance();

            bucket.createOneDocumentInEachVbucket("b");
            streamer.assertMutationCount(batchSize * 2);
        }
    }
}
