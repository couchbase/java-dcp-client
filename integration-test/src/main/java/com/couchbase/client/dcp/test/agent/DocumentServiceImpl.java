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

package com.couchbase.client.dcp.test.agent;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.couchbase.client.dcp.test.util.IntegrationTestHelper.forceKeyToPartition;
import static com.couchbase.client.dcp.test.util.IntegrationTestHelper.upsertWithRetry;
import static com.couchbase.client.dcp.test.util.IntegrationTestHelper.validateJson;
import static java.util.Objects.requireNonNull;

@Service
public class DocumentServiceImpl implements DocumentService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DocumentServiceImpl.class);

    private final CouchbaseCluster cluster;
    private final StreamerService streamerService;
    volatile int cachedPartitionCount;

    @Autowired
    public DocumentServiceImpl(CouchbaseCluster cluster, StreamerService streamerService) {
        this.cluster = requireNonNull(cluster);
        this.streamerService = requireNonNull(streamerService);
    }

    @Override
    public void upsert(String bucket, String documentId, String documentBodyJson) {
        // todo Open the bucket once on startup?
        cluster.openBucket(bucket)
                .upsert(RawJsonDocument.create(documentId, validateJson(documentBodyJson)));
    }

    @Override
    public Set<String> upsertOneDocumentToEachVbucket(String bucket, String documentIdPrefix) {
        final Bucket b = cluster.openBucket(bucket);
        final Set<String> ids = new HashSet<>();

        final int partitionCount = getNumberOfPartitions(bucket);
        for (int i = 0; i < partitionCount; i++) {
            final int partition = i;
            final String id = forceKeyToPartition(documentIdPrefix, partition, partitionCount)
                    .orElseThrow(() -> new RuntimeException("Failed to force id " + documentIdPrefix + " to partition " + partition));
            ids.add(id);
            try {
                upsertWithRetry(b, JsonDocument.create(id, JsonObject.create().put("id", id)));
            } catch (Exception e) {
                LOGGER.error("failed to upsert {}", id, e);
                throw new RuntimeException(e);
            }
        }

        LOGGER.info("Upserted {} documents, one to each vbucket", ids.size());
        if (ids.isEmpty()) {
            throw new AssertionError("Didn't upsert any documents; bad partition count?");
        }
        return ids;
    }

    @Override
    public void delete(String bucket, List<String> documentIds) {
        Bucket b = cluster.openBucket(bucket);

        for (String id : documentIds) {
            try {
                b.remove(id);
            } catch (DocumentDoesNotExistException ignore) {
            }
        }
    }

    // assumes all buckets are configured with same number of partitions
    private int getNumberOfPartitions(String bucket) {
        if (cachedPartitionCount == 0) {
            cachedPartitionCount = streamerService.getNumberOfPartitions(bucket);
        }
        return cachedPartitionCount;
    }
}
