/*
 * Copyright 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.dcp.config;

import com.couchbase.client.dcp.message.HelloRequest;
import com.couchbase.client.dcp.util.Version;
import org.junit.Test;

import static com.couchbase.client.dcp.config.CompressionMode.DISABLED;
import static com.couchbase.client.dcp.config.CompressionMode.DISCRETIONARY;
import static com.couchbase.client.dcp.config.CompressionMode.FORCED;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CompressionModeTest {
    private static final Version OLDER = new Version(4, 0, 0);
    private static final Version WATSON = new Version(4, 5, 0);
    private static final Version VULCAN = new Version(5, 5, 0);

    @Test
    public void effectiveMode() throws Exception {
        assertEquals(DISABLED, CompressionMode.DISABLED.effectiveMode(OLDER));
        assertEquals(DISABLED, CompressionMode.DISABLED.effectiveMode(WATSON));
        assertEquals(DISABLED, CompressionMode.DISABLED.effectiveMode(VULCAN));

        assertEquals(DISABLED, FORCED.effectiveMode(OLDER));
        assertEquals(FORCED, FORCED.effectiveMode(WATSON));
        assertEquals(FORCED, FORCED.effectiveMode(VULCAN));

        assertEquals(DISABLED, DISCRETIONARY.effectiveMode(OLDER));
        assertEquals(DISABLED, DISCRETIONARY.effectiveMode(WATSON));
        assertEquals(DISCRETIONARY, DISCRETIONARY.effectiveMode(VULCAN));
    }

    @Test
    public void vulcanDisabled() throws Exception {
        assertArrayEquals(new short[0], DISABLED.getHelloFeatures(VULCAN));
        assertEquals(emptyMap(), DISABLED.getDcpControls(VULCAN));
    }

    @Test
    public void vulcanForced() throws Exception {
        assertArrayEquals(new short[]{HelloRequest.DATATYPE, HelloRequest.SNAPPY}, FORCED.getHelloFeatures(VULCAN));
        assertEquals(singletonMap("force_value_compression", "true"), FORCED.getDcpControls(VULCAN));
    }

    @Test
    public void vulcanDiscretionary() throws Exception {
        assertArrayEquals(new short[]{HelloRequest.DATATYPE, HelloRequest.SNAPPY}, DISCRETIONARY.getHelloFeatures(VULCAN));
        assertEquals(emptyMap(), DISCRETIONARY.getDcpControls(VULCAN));
    }

    @Test
    public void watsonDisabled() throws Exception {
        // Don't care about hello, doesn't affect server compression behavior
        assertEquals(emptyMap(), DISABLED.getDcpControls(WATSON));
    }

    @Test
    public void watsonForced() throws Exception {
        // Don't care about hello, doesn't affect server compression behavior
        assertEquals(singletonMap("enable_value_compression", "true"), FORCED.getDcpControls(WATSON));
    }

    @Test
    public void olderDisabled() throws Exception {
        // Don't care about hello, doesn't affect server compression behavior
        assertEquals(emptyMap(), DISABLED.getDcpControls(OLDER));
    }
}
