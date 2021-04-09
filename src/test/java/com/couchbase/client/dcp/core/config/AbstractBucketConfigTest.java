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
package com.couchbase.client.dcp.core.config;

import com.couchbase.client.dcp.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the functionality of the {@link AbstractBucketConfig}.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
public class AbstractBucketConfigTest {

    private static final String UUID = "aa4b515529fa706f1e5f09f21abb5c06";
    private static final String NAME = "name";
    private static final BucketNodeLocator LOCATOR = BucketNodeLocator.VBUCKET;
    private static final String URI = "http://foobar:8091/foo";
    private static final String STREAMING_URI = "http://foobar:8091/foo";

    @Test
    void shouldCheckIfServiceIsEnabled() throws Exception {
        List<NodeInfo> nodeInfos = new ArrayList<NodeInfo>();

        Map<ServiceType, Integer> direct = new HashMap<ServiceType, Integer>();
        Map<ServiceType, Integer> ssl = new HashMap<ServiceType, Integer>();

        direct.put(ServiceType.BINARY, 1234);
        direct.put(ServiceType.CONFIG, 1235);
        ssl.put(ServiceType.BINARY, 4567);

        nodeInfos.add(new DefaultNodeInfo("127.0.0.1", direct, ssl, null));

        BucketConfig bc = new SampleBucketConfig(nodeInfos, null);

        assertTrue(bc.serviceEnabled(ServiceType.BINARY));
        assertTrue(bc.serviceEnabled(ServiceType.CONFIG));
        assertFalse(bc.serviceEnabled(ServiceType.QUERY));
        assertFalse(bc.serviceEnabled(ServiceType.VIEW));
    }

    static class SampleBucketConfig extends AbstractBucketConfig {

        public SampleBucketConfig(List<NodeInfo> nodeInfos, List<PortInfo> portInfos) {
            super(UUID, NAME, LOCATOR, URI, STREAMING_URI, nodeInfos, portInfos, null,
                "127.0.0.1");
        }

        @Override
        public boolean tainted() {
            return false;
        }

        @Override
        public long rev() {
            return 0;
        }

        @Override
        public BucketType type() {
            return null;
        }

        @Override
        public boolean hasFastForwardMap() {
            return false;
        }
    }

}
