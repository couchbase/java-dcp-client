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

import com.couchbase.client.dcp.config.HostAndPort;
import com.couchbase.client.dcp.core.config.parser.BucketConfigParser;
import com.couchbase.client.dcp.core.env.NetworkResolution;
import com.couchbase.client.dcp.core.service.ServiceType;
import com.couchbase.client.dcp.core.util.Resources;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that parsing various bucket configs works as expected through the
 * {@link BucketConfigParser}.
 */
public class DefaultCouchbaseBucketConfigTest {
    private static final HostAndPort localhost = new HostAndPort("127.0.0.1", 0);

    @Test
    void shouldHavePrimaryPartitionsOnNode() {
        String raw = Resources.read("config_with_mixed_partitions.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertTrue(config.hasPrimaryPartitionsOnNode("1.2.3.4"));
        assertFalse(config.hasPrimaryPartitionsOnNode("2.3.4.5"));
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
        assertTrue(config.nodes().get(0).alternateAddresses().isEmpty());
    }

    @Test
    void shouldReplaceHostPlaceholder() {
        String raw = Resources.read("config_with_host_placeholder.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, new HostAndPort("example.com", 0));
        assertEquals("example.com", config.nodes().get(0).hostname());
    }

    @Test
    void shouldReplaceHostPlaceholderIpv6() {
        String raw = Resources.read("config_with_host_placeholder.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, new HostAndPort("::1", 0));
        assertEquals("0:0:0:0:0:0:0:1", config.nodes().get(0).hostname());
    }

    @Test
    void shouldFallbackToNodeHostnameIfNotInNodesExt() {
        String raw = Resources.read("nodes_ext_without_hostname.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertEquals(1, config.nodes().size());
        assertEquals("1.2.3.4", config.nodes().get(0).hostname());
        assertEquals(BucketNodeLocator.VBUCKET, config.locator());
        assertFalse(config.ephemeral());
    }

    @Test
    void shouldGracefullyHandleEmptyPartitions() {
        String raw = Resources.read("config_with_no_partitions.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertEquals(DefaultCouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForMaster(24, false));
        assertEquals(DefaultCouchbaseBucketConfig.PARTITION_NOT_EXISTENT, config.nodeIndexForReplica(24, 1, false));
        assertFalse(config.ephemeral());
    }

    @Test
    void shouldLoadEphemeralBucketConfig() {
        String raw = Resources.read("ephemeral_bucket_config.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertTrue(config.ephemeral());
        assertTrue(config.serviceEnabled(ServiceType.BINARY));
        assertFalse(config.serviceEnabled(ServiceType.VIEW));
    }

    @Test
    void shouldLoadConfigWithoutBucketCapabilities() {
        String raw = Resources.read("config_without_capabilities.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertFalse(config.ephemeral());
        assertEquals(0, config.numberOfReplicas());
        assertEquals(64, config.numberOfPartitions());
        assertEquals(2, config.nodes().size());
        assertTrue(config.serviceEnabled(ServiceType.BINARY));
        assertTrue(config.serviceEnabled(ServiceType.VIEW));
    }

    @Test
    void shouldLoadConfigWithSameNodesButDifferentPorts() {
        String raw = Resources.read("cluster_run_two_nodes_same_host.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertFalse(config.ephemeral());
        assertEquals(1, config.numberOfReplicas());
        assertEquals(1024, config.numberOfPartitions());
        assertEquals(2, config.nodes().size());
        assertEquals("192.168.1.194", config.nodes().get(0).hostname());
        assertEquals(9000, (int)config.nodes().get(0).services().get(ServiceType.CONFIG));
        assertEquals("192.168.1.194", config.nodes().get(1).hostname());
        assertEquals(9001, (int)config.nodes().get(1).services().get(ServiceType.CONFIG));
    }

    @Test
    void shouldLoadConfigWithMDS() {
        String raw = Resources.read("cluster_run_three_nodes_mds_with_localhost.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertEquals(3, config.nodes().size());
        assertEquals("192.168.0.102", config.nodes().get(0).hostname());
        assertEquals("127.0.0.1", config.nodes().get(1).hostname());
        assertEquals("127.0.0.1", config.nodes().get(2).hostname());
        assertTrue(config.nodes().get(0).services().containsKey(ServiceType.BINARY));
        assertTrue(config.nodes().get(1).services().containsKey(ServiceType.BINARY));
        assertFalse(config.nodes().get(2).services().containsKey(ServiceType.BINARY));
    }

    @Test
    void shouldLoadConfigWithIPv6() {
        String raw = Resources.read("config_with_ipv6.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, new HostAndPort("::1", 0));

        assertEquals(2, config.nodes().size());
        assertEquals("fd63:6f75:6368:2068:1471:75ff:fe25:a8be", config.nodes().get(0).hostname());
        assertEquals("fd63:6f75:6368:2068:c490:b5ff:fe86:9cf7", config.nodes().get(1).hostname());

        assertEquals(1, config.numberOfReplicas());
        assertEquals(1024, config.numberOfPartitions());
    }

    /**
     * This is a regression test. It has been added to make sure a config with a bucket
     * capability that is not known to the client still makes it parse properly.
     */
    @Test
    void shouldIgnoreUnknownBucketCapabilities() {
        String raw = Resources.read("config_with_invalid_capability.json", getClass());
        BucketConfig config = BucketConfigParser.parse(raw, localhost);
        assertEquals(1, config.nodes().size());
    }

    @Test
    void shouldReadBucketUuid() {
        String raw = Resources.read("config_with_mixed_partitions.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        assertEquals("aa4b515529fa706f1e5f09f21abb5c06", config.uuid());
    }

    @Test
    void shouldHandleMissingBucketUuid() throws Exception {
        String raw = Resources.read("config_without_uuid.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
                BucketConfigParser.parse(raw, localhost);

        assertNull(config.uuid());
    }

    /**
     * This test makes sure that the external hosts are present if set.
     */
    @Test
    void shouldIncludeExternalIfPresent() {
        String raw = Resources.read("config_with_external.json", getClass());
        CouchbaseBucketConfig config = (CouchbaseBucketConfig)
            BucketConfigParser.parse(raw, localhost);

        List<NodeInfo> nodes = config.nodes();
        assertEquals(3, nodes.size());
        for (NodeInfo node : nodes) {
            Map<String, AlternateAddress> addrs = node.alternateAddresses();
            assertEquals(1, addrs.size());
            AlternateAddress addr = addrs.get(NetworkResolution.EXTERNAL.name());
            assertNotNull(addr.hostname());
            assertFalse(addr.services().isEmpty());
            assertFalse(addr.sslServices().isEmpty());
            for (int port : addr.services().values()) {
                assertTrue(port > 0);
            }
            for (int port : addr.sslServices().values()) {
                assertTrue(port > 0);
            }
        }
    }
}
