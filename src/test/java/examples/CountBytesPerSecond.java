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
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.message.DcpMutationMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CountBytesPerSecond {

    public static void main(String... args) throws Exception {

        final AtomicInteger numMutations = new AtomicInteger(0);
        final AtomicLong numBytes = new AtomicLong(0);
        Client client = Client
            .configure()
           // .clusterAt(Arrays.asList("10.142.150.101"))
            .bucket("default")
            .dataEventHandler(new DataEventHandler() {
                @Override
                public void onEvent(ByteBuf event) {
                   // System.err.println(event);
                    if (DcpMutationMessage.is(event)) {
                        numMutations.incrementAndGet();
                        numBytes.addAndGet(event.readableBytes());
                    }
                    event.release();
                }
            })
            .build();

        client.connect().await();

        Thread.sleep(3000); // TODO: fixme startup without delay

        client.startFromBeginning().subscribe();

        long start = 0;
        while(true) {
            if (numMutations.get() == 0) {
                continue;
            }

            if (numMutations.get() == 789600) {
                break;
            }

            if (start == 0) {
                start = System.nanoTime();
            }
        }
        long end = System.nanoTime();

        System.err.println(TimeUnit.NANOSECONDS.toMillis(end - start));
        System.err.println("Loaded MBytes: " + numBytes.get() / 1024 / 1024);
    }
}
