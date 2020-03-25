/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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
import com.couchbase.client.dcp.message.RollbackMessage;
import com.couchbase.client.dcp.transport.netty.ChannelFlowController;
import com.couchbase.client.dcp.deps.io.netty.buffer.ByteBuf;
import rx.CompletableSubscriber;
import rx.Subscription;

import java.util.concurrent.TimeUnit;

/**
 * This example starts from the current point in time and prints every change that happens.
 * <p>
 * Example output from the log when a document is modified and then deleted:
 * <p>
 * Mutation: MutationMessage [key: "airline_10226", vbid: 7, cas: 820685701775360, bySeqno: 490, revSeqno: 11,
 * flags: 0, expiry: 0, lockTime: 0, clength: 171]
 * Deletion: DeletionMessage [key: "airline_10226", vbid: 7, cas: 820691821527040, bySeqno: 491, revSeqno: 12]
 */
public class PrintIncomingChanges {

  public static void main(String[] args) throws Exception {

    // Connect to localhost and use the travel-sample bucket
    final Client client = Client.builder()
        .seedNodes("localhost")
        .bucket("travel-sample")
        .credentials("Administrator", "password")
        .build();

    // If we are in a rollback scenario, rollback the partition and restart the stream.
    client.controlEventHandler(new ControlEventHandler() {
      @Override
      public void onEvent(final ChannelFlowController flowController, final ByteBuf event) {
        if (DcpSnapshotMarkerRequest.is(event)) {
          flowController.ack(event);
        }
        if (RollbackMessage.is(event)) {
          final short partition = RollbackMessage.vbucket(event);
          client.rollbackAndRestartStream(partition, RollbackMessage.seqno(event))
              .subscribe(new CompletableSubscriber() {
                @Override
                public void onCompleted() {
                  System.out.println("Rollback for partition " + partition + " complete!");
                }

                @Override
                public void onError(Throwable e) {
                  System.err.println("Rollback for partition " + partition + " failed!");
                  e.printStackTrace();
                }

                @Override
                public void onSubscribe(Subscription d) {
                }
              });
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
    client.connect().await();

    // Initialize the state (start now, never stop)
    client.initializeState(StreamFrom.NOW, StreamTo.INFINITY).await();

    // Start streaming on all partitions
    client.startStreaming().await();

    // Sleep for some time to print the mutations
    // The printing happens on the IO threads!
    Thread.sleep(TimeUnit.MINUTES.toMillis(10));

    // Once the time is over, shutdown.
    client.disconnect().await();
  }

}
