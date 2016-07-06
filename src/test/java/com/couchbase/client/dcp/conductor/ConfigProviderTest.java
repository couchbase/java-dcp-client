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
import org.junit.Test;
import rx.functions.Action1;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration Test for the {@link ConfigProvider}.
 */
public class ConfigProviderTest {

    @Test
    public void shouldFetchConfigFromNode() throws Exception {
        ConfigProvider configProvider = new ConfigProvider(Arrays.asList("127.0.0.1"), "default", "");

        final AtomicReference<CouchbaseBucketConfig> config = new AtomicReference<CouchbaseBucketConfig>();
        final CountDownLatch latch = new CountDownLatch(1);
        configProvider.configs().forEach(new Action1<CouchbaseBucketConfig>() {
            @Override
            public void call(CouchbaseBucketConfig couchbaseBucketConfig) {
                config.set(couchbaseBucketConfig);
                latch.countDown();
            }
        });

        configProvider.start();

        latch.await(5, TimeUnit.SECONDS);
        CouchbaseBucketConfig found = config.get();
        assertNotNull(found);
        assertEquals("default", found.name());
    }

    @Test
    public void shouldFallbackToNextInList() throws Exception {
        ConfigProvider configProvider = new ConfigProvider(Arrays.asList("enoent", "127.0.0.1"), "default", "");

        final AtomicReference<CouchbaseBucketConfig> config = new AtomicReference<CouchbaseBucketConfig>();
        final CountDownLatch latch = new CountDownLatch(1);
        configProvider.configs().forEach(new Action1<CouchbaseBucketConfig>() {
            @Override
            public void call(CouchbaseBucketConfig couchbaseBucketConfig) {
                config.set(couchbaseBucketConfig);
                latch.countDown();
            }
        });

        configProvider.start();

        latch.await(5, TimeUnit.SECONDS);
        CouchbaseBucketConfig found = config.get();
        assertNotNull(found);
        assertEquals("default", found.name());
    }

}