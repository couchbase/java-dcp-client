/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.dcp.benchmark;

import com.couchbase.client.dcp.Client;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class ConnectAndStreamBenchmark {

    /**
     * Connects to a Couchbase server and stream the DCP updates for the "travel-sample" bucket.
     */
    @Benchmark
    public void testMethod(Blackhole blackhole) throws Exception {
        Client client = BenchmarkHelper.createConnectedClient(blackhole);

        BenchmarkHelper.streamAndAwaitCompletion(client);

        client.disconnect().await();
    }
}


