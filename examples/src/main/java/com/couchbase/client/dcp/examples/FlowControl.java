/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.dcp.examples;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This sample shows how to set flow control and acknowledge bytes as they arrive to keep going.
 * <p>
 * If you comment out the acknowledge part, you'll see no more changes streamed since the server waits forever
 * for acknowledgements.
 */
public class FlowControl {

  public static void main(String[] args) throws Exception {
    // Connect to localhost and use the travel-sample bucket
    final Client client = Client.builder()
        .seedNodes("localhost")
        .bucket("travel-sample")
        .credentials("Administrator", "password")
        .controlParam(DcpControl.Names.CONNECTION_BUFFER_SIZE, 10000) // set the buffer to 10K
        .bufferAckWatermark(75) // after 75% are reached of the 10KB, acknowledge against the server
        .build();

    // Don't do anything with control events in this example
    client.controlEventHandler(new ControlEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpSnapshotMarkerRequest.is(event)) {
          flowController.ack(event);
        }
        event.release();
      }
    });

    // Acknowledge bytes to let it move on...
    final AtomicLong changes = new AtomicLong(0);
    client.dataEventHandler(new DataEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        // this method will acknowledge the bytes for mutation, deletion and expiration
        flowController.ack(event);
        changes.incrementAndGet();
        event.release();
      }
    });

    // Connect the sockets
    client.connect().block();

    // Initialize the state (start now, never stop)
    client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).block();

    // Start streaming on all partitions
    client.startStreaming().block();

    // ZZzzzZZ
    while (true) {
      System.out.println("Saw " + changes.get() + " changes so far.");
      Thread.sleep(1000);
    }

  }
}
