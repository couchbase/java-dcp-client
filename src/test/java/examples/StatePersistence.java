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

import com.couchbase.client.dcp.*;
import com.couchbase.client.dcp.message.DcpDeletionMessage;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.dcp.state.StateFormat;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.concurrent.TimeUnit;

/**
 * In this example every minute the state is persisted to a file. If on startup the file is found it is used to
 * pick up from where it left off, otherwise it starts from the beginning.
 *
 * This program works well together with the {@link WorkloadGenerator} which runs some constant workload and then
 * this file is stopped and resumed to showcase the pickup.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class StatePersistence {

    static String FILENAME = "state.json";

    public static void main(String[] args) throws Exception {
        // Connect to localhost and use the travel-sample bucket
        final Client client = Client.configure()
            .hostnames("localhost")
            .bucket("travel-sample")
            .build();

        // Don't do anything with control events in this example
        client.controlEventHandler(new ControlEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                event.release();
            }
        });

        // Print out Mutations and Deletions
        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (DcpMutationMessage.is(event)) {
                    System.out.println("Mutation: " + DcpMutationMessage.toString(event));
                    // You can print the content via DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8);
                } else if (DcpDeletionMessage.is(event)) {
                    System.out.println("Deletion: " + DcpDeletionMessage.toString(event));
                }
                event.release();
            }
        });

        // Connect the sockets
        client.connect().await();

        // Try to load the persisted state from file if it exists
        File file = new File(FILENAME);
        byte[] persisted = null;
        if (file.exists()) {
            FileInputStream fis = new FileInputStream(FILENAME);
            persisted = IOUtils.toByteArray(fis);
            fis.close();
        }

        // if the persisted file exists recover, if not start from beginning
        client.recoverOrInitializeState(StateFormat.JSON, persisted, StreamFrom.BEGINNING, StreamTo.INFINITY).await();

        // Start streaming on all partitions
        client.startStreaming().await();

        // Persist the State ever 10 seconds
        while(true) {
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
