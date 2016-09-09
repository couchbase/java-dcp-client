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
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.CharsetUtil;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A little more involved sample which uses JSON decoding to aggregate the number of airports in france.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class AirportsInFrance {

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

        // Collect the statistics on mutations
        final AtomicInteger numAirports = new AtomicInteger(0);

        client.dataEventHandler(new DataEventHandler() {
            @Override
            public void onEvent(ByteBuf event) {
                if (DcpMutationMessage.is(event)) {
                    // Using the Java SDKs JsonObject for simple access to the document
                    JsonObject content = JsonObject.fromJson(
                        DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8)
                    );
                    if (content.getString("type").equals("airport")
                        && content.getString("country").toLowerCase().equals("france")) {
                        numAirports.incrementAndGet();
                    }
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
        while(true) {
            if (client.sessionState().isAtEnd()) {
                break;
            }
            Thread.sleep(500);
        }

        System.out.println("Number of Airports in France: " + numAirports.get());

        // Proper Shutdown
        client.disconnect().await();
    }
}
