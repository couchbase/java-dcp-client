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
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.Arrays;

public class CountBytesPerSecond {

    public static void main(String... args) throws Exception {
        Client client = Client
            .configure()
           // .clusterAt(Arrays.asList("10.142.150.101"))
            .bucket("travel-sample")
            .dataEventHandler(new DataEventHandler() {
                @Override
                public void onEvent(ByteBuf event) {
                    System.err.println(event);
                }
            })
            .build();

        client.connect().await();

        Thread.sleep(1000); // TODO: fixme startup without delay

        client.startFromBeginning().await();

        Thread.sleep(100000);
    }
}
