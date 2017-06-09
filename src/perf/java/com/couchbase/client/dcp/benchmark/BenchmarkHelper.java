/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.dcp.benchmark;

import java.util.concurrent.CountDownLatch;

import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Utility methods common to the different benchmarks.
 */
class BenchmarkHelper {
    private BenchmarkHelper() {
    }

    /**
     * Returns a connected client configured to read all data and control events and send the
     * results to given black hole.
     */
    static Client createConnectedClient(final Blackhole blackhole) {
        Client client = Client.configure()
                .hostnames("localhost")
                .bucket("travel-sample")
                .build();

        // Don't do anything with control events in this example
        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                if (DcpSnapshotMarkerRequest.is(event)) {
                    flowController.ack(event);
                }
                readAndConsume(event, blackhole);
                event.release();
            }
        });

        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ChannelFlowController flowController, ByteBuf event) {
                readAndConsume(event, blackhole);
                event.release();
            }
        });

        // Connect the sockets
        client.connect().await();

        return client;
    }

    /**
     * Copies the contents of the buffer to a byte array and sends that array to the black hole.
     */
    private static void readAndConsume(ByteBuf buffer, Blackhole blackhole) {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(0, bytes);
        blackhole.consume(bytes);
    }

    /**
     * Streams from beginning to now. Waits for all partitions to finish streaming.
     *
     * @param client a connected client
     */
    static void streamAndAwaitCompletion(Client client) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(client.numPartitions());

        client.systemEventHandler(new SystemEventHandler() {
            @Override
            public void onEvent(CouchbaseEvent event) {
                if (event instanceof StreamEndEvent) {
                    latch.countDown();
                }
            }
        });

        client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).await();
        client.startStreaming().await();

        // Wait until all partition streams have ended
        latch.await();
    }
}
