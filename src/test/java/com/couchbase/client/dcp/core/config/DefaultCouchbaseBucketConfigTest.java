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
import com.couchbase.client.dcp.core.env.NetworkResolution;
import com.couchbase.client.dcp.core.service.ServiceType;
import com.couchbase.client.dcp.core.util.Resources;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that parsing various bucket configs works as expected through the
 * {@link CouchbaseBucketConfigParser}.
 */
public class DefaultCouchbaseBucketConfigTest {
    @Test
    @Disabled("The input JSON is from a version of Couchbase Server that's too old.")
    void shouldHavePrimaryPartitionsOnNode() {
        CouchbaseBucketConfig config = read("config_with_mixed_partitions.json");

        assertTrue(config.hasPrimaryPartitionsOnNode("1.2.3.4"));
        assertFalse(config.hasPrimaryPartitionsOnNode("2.3.4.5"));
        assertFalse(config.ephemeral());
        //assertTrue(config.nodes().get(0).alternateAddresses().isEmpty());
    }

    @Test
    void shouldReplaceHostPlaceholder() {
        CouchbaseBucketConfig config = read("config_with_host_placeholder.json", "example.com");
        assertEquals("example.com", config.nodes().get(0).hostname());
    }

    @Test
    void shouldReplaceHostPlaceholderIpv6() {
        CouchbaseBucketConfig config = read("config_with_host_placeholder.json", new HostAndPort("::1", 0).host());
        assertEquals("0:0:0:0:0:0:0:1", config.nodes().get(0).hostname());
    }

    @Disabled("The input JSON is from a version of Couchbase Server that's too old.")
    @Test
    void shouldFallbackToNodeHostnameIfNotInNodesExt() {
        CouchbaseBucketConfig config = read("nodes_ext_without_hostname.json");

        assertEquals(1, config.nodes().size());
        assertEquals("1.2.3.4", config.nodes().get(0).hostname());
        assertFalse(config.ephemeral());
    }

    @Test
    void shouldGracefullyHandleEmptyPartitions() {
        CouchbaseBucketConfig config = read("config_with_no_partitions.json");

        assertEquals(-2, config.nodeIndexForActive(24, false));
        assertEquals(-2, config.nodeIndexForReplica(24, 1, false));
        assertFalse(config.ephemeral());
    }

    @Test
    void shouldLoadEphemeralBucketConfig() {
        CouchbaseBucketConfig config = read("ephemeral_bucket_config.json");

        assertTrue(config.ephemeral());
        assertTrue(config.serviceEnabled(ServiceType.KV));
        assertTrue(config.serviceEnabled(ServiceType.VIEWS));
    }

    @Test
    void shouldLoadConfigWithSameNodesButDifferentPorts() {
        CouchbaseBucketConfig config = read("cluster_run_two_nodes_same_host.json");

        assertFalse(config.ephemeral());
        assertEquals(1, config.numberOfReplicas());
        assertEquals(1024, config.partitions().size());
        assertEquals(2, config.nodes().size());
        assertEquals("192.168.1.194", config.nodes().get(0).host());
        assertEquals(OptionalInt.of(9000), config.nodes().get(0).port(ServiceType.MANAGER));
        assertEquals("192.168.1.194", config.nodes().get(1).host());
        assertEquals(OptionalInt.of(9001), config.nodes().get(1).port(ServiceType.MANAGER));
    }

    @Disabled("The input JSON is from a version of Couchbase Server that's too old.")
    @Test
    void shouldLoadConfigWithMDS() {
        CouchbaseBucketConfig config = read("cluster_run_three_nodes_mds_with_localhost.json");

        assertEquals(3, config.nodes().size());
        assertEquals("192.168.0.102", config.nodes().get(0).hostname());
        assertEquals("127.0.0.1", config.nodes().get(1).hostname());
        assertEquals("127.0.0.1", config.nodes().get(2).hostname());
        assertTrue(config.nodes().get(0).services().containsKey(ServiceType.KV));
        assertTrue(config.nodes().get(1).services().containsKey(ServiceType.KV));
        assertFalse(config.nodes().get(2).services().containsKey(ServiceType.KV));
    }

    @Test
    void shouldLoadConfigWithIPv6() {
        CouchbaseBucketConfig config = read("config_with_ipv6.json", new HostAndPort("::1",0).host());

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
        CouchbaseBucketConfig config = read("config_with_invalid_capability.json");
        assertEquals(1, config.nodes().size());
    }

    @Disabled("The input JSON is from a version of Couchbase Server that's too old.")
    @Test
    void shouldReadBucketUuid() {
        CouchbaseBucketConfig config = read("config_with_mixed_partitions.json");
        assertEquals("aa4b515529fa706f1e5f09f21abb5c06", config.uuid());
    }

    @Disabled("All supported versions of Couchbase Server always include a UUID")
    @Test
    void shouldHandleMissingBucketUuid() throws Exception {
        CouchbaseBucketConfig config = read("config_without_uuid.json");
        assertNull(config.uuid());
    }

    /**
     * This test makes sure that the external hosts are present if set.
     */
    @Test
    void shouldIncludeExternalIfPresent() {
        CouchbaseBucketConfig config = read("config_with_external.json", "127.0.0.1", PortSelector.NON_TLS, NetworkSelector.EXTERNAL);

        List<NodeInfo> nodes = config.nodes();
        assertEquals(3, nodes.size());
        assertEquals(NetworkResolution.EXTERNAL, config.globalConfig().network());

        for (NodeInfo node : nodes) {
            assertFalse(node.inaccessible());
            assertFalse(node.ports().isEmpty());
            for (int port : node.ports().values()) {
                assertTrue(port > 0);
            }
        }

        assertEquals(32790, nodes.get(0).port(ServiceType.MANAGER).orElse(0));

        // Again, TLS port this time
        config = read("config_with_external.json", "127.0.0.1", PortSelector.TLS, NetworkSelector.EXTERNAL);
        assertEquals(32773, config.nodes().get(0).port(ServiceType.MANAGER).orElse(0));
    }

    private static CouchbaseBucketConfig read(String resourceName) {
        return read(resourceName, "127.0.0.1");
    }
    private static CouchbaseBucketConfig read(String resourceName, String originHost) {
        return read(resourceName, originHost, PortSelector.NON_TLS, NetworkSelector.DEFAULT);
    }

    private static CouchbaseBucketConfig read(String resourceName, String originHost, PortSelector portSelector, NetworkSelector networkSelector) {
        String json = Resources.read(resourceName, DefaultCouchbaseBucketConfigTest.class);
        return CouchbaseBucketConfigParser.parse(
            json.getBytes(UTF_8),
            originHost,
            portSelector,
            networkSelector
        );
    }
}
