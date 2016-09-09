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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.UUID;

/**
 * A simple helper class using the SDK to generate some load on the bucket!
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class WorkloadGenerator {

    public static void main(String... args) throws Exception {
        CouchbaseCluster cluster = CouchbaseCluster.create("127.0.0.1");
        Bucket bucket = cluster.openBucket("default");

        while(true) {
            for (int i = 0; i < 1024; i++) {
                bucket.upsert(JsonDocument.create("doc:"+i, JsonObject.create().put("uuid", UUID.randomUUID().toString())));
                Thread.sleep(1000);
            }
        }
    }
}
