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
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.message.DcpSnapshotMarkerRequest;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;

/**
 * In this example every minute the state is persisted to a file. If on startup the file is found it is used to
 * pick up from where it left off, otherwise it starts from the beginning.
 * <p>
 * This program works well together with the {@link WorkloadGenerator} which runs some constant workload and then
 * this file is stopped and resumed to showcase the pickup.
 */
public class StatePersistence {

  static String BUCKET = "travel-sample";
  static String FILENAME = "state-" + BUCKET + ".json";

  public static void main(String[] args) throws Exception {
    // Connect to localhost and use the travel-sample bucket
    final Client client = Client.builder()
        .seedNodes("localhost")
        .bucket(BUCKET)
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

    // Print out Mutations and Deletions
    client.dataEventHandler(new DataEventHandler() {
      @Override
      public void onEvent(ChannelFlowController flowController, ByteBuf event) {
        if (DcpMutationMessage.is(event)) {
          System.out.println("Mutation: " + DcpMutationMessage.toString(event));
          // You can print the content via DcpMutationMessage.content(event).toString(StandardCharsets.UTF_8);
        } else if (DcpDeletionMessage.is(event)) {
          System.out.println("Deletion: " + DcpDeletionMessage.toString(event));
        }
        event.release();
      }
    });

    // Connect the sockets
    client.connect().block();

    // Try to load the persisted state from file if it exists
    File file = new File(FILENAME);
    byte[] persisted = null;
    if (file.exists()) {
      FileInputStream fis = new FileInputStream(FILENAME);
      persisted = IOUtils.toByteArray(fis);
      fis.close();
    }

    // if the persisted file exists recover, if not start from beginning
    client.recoverOrInitializeState(StateFormat.JSON, persisted, StreamFrom.BEGINNING, StreamTo.INFINITY).block();

    // Start streaming on all partitions
    client.startStreaming().block();

    // Persist the State ever 10 seconds
    while (true) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(10));

      // export the state as a JSON byte array
      byte[] state = client.sessionState().export(StateFormat.JSON);

      // Write it to a file
      FileOutputStream output = new FileOutputStream(new File(FILENAME));
      IOUtils.write(state, output);
      output.close();

      System.out.println(System.currentTimeMillis() + " - Persisted State!");
    }
  }

}
