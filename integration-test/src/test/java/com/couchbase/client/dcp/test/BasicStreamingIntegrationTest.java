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

public class BasicStreamingIntegrationTest extends DcpIntegrationTestBase {
    private static final int BATCH_SIZE = 1024;

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
            bucket.createDocuments(BATCH_SIZE, "a");

            try (RemoteDcpStreamer streamer = bucket.newStreamer()
                    .range(StreamFrom.NOW, StreamTo.INFINITY)
                    .start()) {

                streamer.assertMutationCount(0);
                bucket.createDocuments(BATCH_SIZE, "b");
                streamer.assertMutationCount(BATCH_SIZE);
            }
        }
    }

    @Test
    public void canStreamFromBeginningToInfinity() throws Exception {
        try (TestBucket bucket = newBucket().create()) {
            bucket.createDocuments(BATCH_SIZE, "a");

            try (RemoteDcpStreamer streamer = bucket.newStreamer().start()) {
                streamer.assertMutationCount(BATCH_SIZE);
                bucket.createDocuments(BATCH_SIZE, "b");
                streamer.assertMutationCount(BATCH_SIZE * 2);
            }
        }
    }

    @Test
    public void rollbackMitigationWillBufferUnpersistedEvents() throws Exception {
        try (TestBucket bucket = newBucket().create()) {
            try (RemoteDcpStreamer streamer = bucket.newStreamer()
                    .mitigateRollbacks()
                    .start()) {

                bucket.createDocuments(BATCH_SIZE, "a");
                bucket.stopPersistence();
                bucket.createDocuments(BATCH_SIZE, "b");

                // expect to see all of "a" and none of "b"
                streamer.assertMutationCount(BATCH_SIZE);

                bucket.startPersistence();

                // Now wait and expect both batches
                streamer.assertMutationCount(BATCH_SIZE * 2);
            }
        }
    }
}
