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

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.RawJsonTranscoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.couchbase.client.dcp.core.utils.CbCollections.mapOf;
import static com.couchbase.client.dcp.test.util.IntegrationTestHelper.forceKeyToPartition;
import static com.couchbase.client.dcp.test.util.IntegrationTestHelper.validateJson;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static java.util.Objects.requireNonNull;

@Service
public class DocumentServiceImpl implements DocumentService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentServiceImpl.class);

  private final ClusterSupplier clusterSupplier;
  private final StreamerService streamerService;
  volatile int cachedPartitionCount;

  @Autowired
  public DocumentServiceImpl(ClusterSupplier clusterSupplier, StreamerService streamerService) {
    this.clusterSupplier = requireNonNull(clusterSupplier);
    this.streamerService = requireNonNull(streamerService);
  }

  private Cluster cluster() {
    return clusterSupplier.get();
  }

  @Override
  public void upsert(String bucket, String documentId, String documentBodyJson) {
    // todo Open the bucket once on startup?
    cluster().bucket(bucket)
        .defaultCollection()
        .upsert(documentId, validateJson(documentBodyJson),
            upsertOptions().transcoder(RawJsonTranscoder.INSTANCE));
  }

  @Override
  public Set<String> upsertOneDocumentToEachVbucket(String bucket, String documentIdPrefix) {
    final Collection c = cluster().bucket(bucket).defaultCollection();
    final Set<String> ids = new HashSet<>();

    final int partitionCount = getNumberOfPartitions(bucket);
    for (int i = 0; i < partitionCount; i++) {
      final int partition = i;
      final String id = forceKeyToPartition(documentIdPrefix, partition, partitionCount)
          .orElseThrow(() -> new RuntimeException("Failed to force id " + documentIdPrefix + " to partition " + partition));
      ids.add(id);
      try {
        c.upsert(id, mapOf("id", id));
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
    Collection c = cluster().bucket(bucket).defaultCollection();

    for (String id : documentIds) {
      try {
        c.remove(id);
      } catch (DocumentNotFoundException ignore) {
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
