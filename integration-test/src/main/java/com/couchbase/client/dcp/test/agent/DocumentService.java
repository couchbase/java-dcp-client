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

import com.github.therapi.core.annotation.Remotable;

import java.util.List;

@Remotable("document")
public interface DocumentService {
    /**
     * @param bucket Name of the bucket to upsert into.
     * @param documentId ID to assign to the document.
     * @param documentBodyJson Body to assign to the document.
     */
    void upsert(String bucket, String documentId, String documentBodyJson);

    /**
     * Deletes documents if they exist, otherwise does nothing.
     *
     * @param bucket Name of the bucket to delete from.
     * @param documentIds Document IDs to delete.
     */
    void delete(String bucket, List<String> documentIds);
}
