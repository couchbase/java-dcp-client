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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;

import java.util.UUID;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A simple helper class using the SDK to generate some load on the bucket!
 */
public class WorkloadGenerator {

  public static void main(String... args) throws Exception {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("default");
    Collection collection = bucket.defaultCollection();

    while (true) {
      for (int i = 0; i < 1024; i++) {
        String docId = "doc:" + i;
        collection.upsert(docId, singletonMap("uuid", UUID.randomUUID().toString()));
        SECONDS.sleep(1);
      }
    }
  }
}
