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
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Service
public class DocumentServiceImpl implements DocumentService {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final CouchbaseCluster cluster;

    @Autowired
    public DocumentServiceImpl(CouchbaseCluster cluster) {
        this.cluster = requireNonNull(cluster);
    }

    @Override
    public void upsert(String bucket, String documentId, String documentBodyJson) {
        // todo Open the bucket once on startup?
        cluster.openBucket(bucket)
                .upsert(RawJsonDocument.create(documentId, validateJson(documentBodyJson)));
    }

    private static String validateJson(String json) {
        try {
            try (JsonParser jp = objectMapper.getFactory().createParser(json)) {
                objectMapper.readTree(jp);
                if (jp.nextToken() != null) {
                    throw new IllegalArgumentException("JSON syntax error; trailing garbage detected");
                }
                return json;
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
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
}
