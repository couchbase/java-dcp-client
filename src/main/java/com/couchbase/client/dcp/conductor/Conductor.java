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
package com.couchbase.client.dcp.conductor;

import com.couchbase.client.core.config.CouchbaseBucketConfig;
import rx.Completable;
import rx.functions.Action1;

import java.util.Arrays;

public class Conductor {

    private final ConfigProvider configProvider;

    public Conductor(String hostname, String bucket, String password) {
        configProvider = new ConfigProvider(Arrays.asList(hostname), bucket, password);
        configProvider.configs().forEach(new Action1<CouchbaseBucketConfig>() {
            @Override
            public void call(CouchbaseBucketConfig config) {
                reconfigure(config);
            }
        });
    }

    public Completable connect() {
        return configProvider.start();
    }

    public void stop() {
        configProvider.stop();
    }

    private void reconfigure(CouchbaseBucketConfig config) {
        System.err.println("RECONFIGURE!");
        System.err.println(config);

        // iterate nodes and add/remove
    }
}
