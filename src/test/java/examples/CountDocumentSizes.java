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
package examples;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.StreamFrom;
import com.couchbase.client.dcp.StreamTo;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This application streams all data from the bucket up until now and accumulates the total size of the
 * document bodies (not accounting for metadata) and prints it out statistics.
 * <p>
 * On the travel-sample bucket you'll see:
 * <p>
 * Total Docs: 31590
 * Total Bytes: 36185411
 * Average Size per Doc: 1145b
 */
public class CountDocumentSizes {

  public static void main(String[] args) throws Exception {

    // Connect to localhost and use the travel-sample bucket
    final Client client = Client.builder()
        .hostnames("localhost")
        .bucket("travel-sample")
        .credentials("Administrator", "password")
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

    // Collect the statistics on mutations
    final AtomicLong totalSize = new AtomicLong(0);
    final AtomicLong docCount = new AtomicLong(0);

    client.dataEventHandler(new DataEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpMutationMessage.is(event)) {
          docCount.incrementAndGet();
          totalSize.addAndGet(DcpMutationMessage.content(event).readableBytes());
        }
        event.release();
      }
    });

    // Connect the sockets
    client.connect().await();

    // Initialize the state (start now, never stop)
    client.initializeState(StreamFrom.BEGINNING, StreamTo.NOW).await();

    // Start streaming on all partitions
    client.startStreaming().await();

    // Sleep and wait until the DCP stream has caught up with the time where we said "now".
    while (true) {
      if (client.sessionState().isAtEnd()) {
        break;
      }
      Thread.sleep(500);
    }

    System.out.println("Total Docs: " + docCount.get());
    System.out.println("Total Bytes: " + totalSize.get());
    System.out.println("Average Size per Doc: " + totalSize.get() / docCount.get() + "b");

    // Proper Shutdown
    client.disconnect().await();
  }
}
