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

import com.couchbase.client.dcp.core.event.CouchbaseEvent;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.events.StreamEndEvent;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.java.document.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A little more involved sample which uses JSON decoding to aggregate the number of airports in france.
 */
public class AirportsInFrance {

  public static void main(String[] args) throws Exception {

    // Connect to localhost and use the travel-sample bucket
    final Client client = Client.builder()
        .seedNodes("localhost")
        .bucket("travel-sample")
        .credentials("Administrator", "password")
        .build();

    // Don't do anything with control events in this example
    client.controlEventHandler(new ControlEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        flowController.ack(event);
        event.release();
      }
    });

    // Collect the statistics on mutations
    final AtomicInteger numAirports = new AtomicInteger(0);

    client.dataEventHandler(new DataEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpMutationMessage.is(event)) {
          // Using the Java SDKs JsonObject for simple access to the document
          JsonObject content = JsonObject.fromJson(
              DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8)
          );
          if ("airport".equals(content.getString("type"))
              && content.getString("country").toLowerCase().equals("france")) {
            numAirports.incrementAndGet();
          }
        }
        event.release();
      }
    });
    client.systemEventHandler(new SystemEventHandler() {
      @Override
      public void onEvent(CouchbaseEvent event) {
        if (event instanceof StreamEndEvent) {
          StreamEndEvent streamEnd = (StreamEndEvent) event;
          if (streamEnd.partition() == 42) {
            System.out.println("Stream for partition 42 has ended (reason: " + streamEnd.reason() + ")");
          }
        }
      }
    });

    // Connect the sockets
    client.connect().block();

    // Initialize the state (start now, never stop)
    client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).block();

    // Start streaming on all partitions
    client.startStreaming().block();

    // Sleep and wait until the DCP stream has caught up with the time where we said "now".
    while (true) {
      if (client.sessionState().isAtEnd()) {
        break;
      }
      Thread.sleep(500);
    }

    System.out.println("Number of Airports in France: " + numAirports.get());

    // Proper Shutdown
    client.disconnect().block();
  }
}
