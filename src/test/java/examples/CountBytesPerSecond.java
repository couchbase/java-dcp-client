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

import com.couchbase.client.core.config.parser.BucketConfigParser;
import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.config.DcpControl;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

import java.util.concurrent.TimeUnit;

public class CountBytesPerSecond {

    public static void main(String... args) throws Exception {


        String input = "{\"rev\":228,\"name\":\"default\",\"uri\":\"/pools/default/buckets/default?bucket_uuid=6ad336ca344ef4705cb821e62d825591\",\"streamingUri\":\"/pools/default/bucketsStreaming/default?bucket_uuid=6ad336ca344ef4705cb821e62d825591\",\"nodes\":[{\"couchApiBase\":\"http://$HOST:8092/default%2B6ad336ca344ef4705cb821e62d825591\",\"hostname\":\"$HOST:8091\",\"ports\":{\"proxy\":11211,\"direct\":11210}}],\"nodesExt\":[{\"services\":{\"mgmt\":8091,\"mgmtSSL\":18091,\"fts\":8094,\"indexAdmin\":9100,\"indexScan\":9101,\"indexHttp\":9102,\"indexStreamInit\":9103,\"indexStreamCatchup\":9104,\"indexStreamMaint\":9105,\"capiSSL\":18092,\"capi\":8092,\"kvSSL\":11207,\"projector\":9999,\"kv\":11210,\"moxi\":11211,\"n1ql\":8093,\"n1qlSSL\":18093},\"thisNode\":true}],\"nodeLocator\":\"vbucket\",\"uuid\":\"6ad336ca344ef4705cb821e62d825591\",\"ddocs\":{\"uri\":\"/pools/default/buckets/default/ddocs\"},\"vBucketServerMap\":{\"hashAlgorithm\":\"CRC\",\"numReplicas\":1,\"serverList\":[\"$HOST:11210\"],\"vBucketMap\":[[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1],[0,-1]]},\"bucketCapabilitiesVer\":\"\",\"bucketCapabilities\":[\"cbhello\",\"touch\",\"couchapi\",\"cccp\",\"xdcrCheckpointing\",\"nodesExt\",\"dcp\"]}";
        System.err.println(BucketConfigParser.parse(input.replace("$HOST", "127.0.0.1")));

/*

        Client client = Client
            .configure()
            .clusterAt("127.0.0.1")
            .bucket("travel-sample")
            .dataEventHandler(new DataEventHandler() {
                @Override
                public void onEvent(ByteBuf event) {
                    System.err.println(event);
                }
            })
            .build();

      client.connect();

        Thread.sleep(100000);*/
    }
}
