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

package com.couchbase.client.dcp.test;

import com.couchbase.client.dcp.test.agent.BucketService;
import com.couchbase.client.dcp.test.agent.DocumentService;
import com.couchbase.client.dcp.test.agent.StreamerService;
import com.github.therapi.jsonrpc.client.JdkHttpClient;
import com.github.therapi.jsonrpc.client.ServiceFactory;

import java.net.MalformedURLException;

import static com.github.therapi.jackson.ObjectMappers.newLenientObjectMapper;

/**
 * Client for the JSON-RPC API exposed by the test agent.
 */
public class RemoteAgent {
    private final BucketService bucketService;
    private final DocumentService documentService;
    private final StreamerService streamerService;

    public RemoteAgent(AgentContainer agentContainer) {
        this("http://localhost:" + agentContainer.getHttpPort() + "/jsonrpc");
    }

    public RemoteAgent(String jsonRpcEndpoint) {
        try {
            ServiceFactory serviceFactory = new ServiceFactory(
                    newLenientObjectMapper(), new JdkHttpClient(jsonRpcEndpoint));

            this.bucketService = serviceFactory.createService(BucketService.class);
            this.documentService = serviceFactory.createService(DocumentService.class);
            this.streamerService = serviceFactory.createService(StreamerService.class);

        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public BucketService bucket() {
        return bucketService;
    }

    public StreamerService streamer() {
        return streamerService;
    }

    public DocumentService document() {
        return documentService;
    }
}
