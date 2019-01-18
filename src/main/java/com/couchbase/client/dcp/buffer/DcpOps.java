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

package com.couchbase.client.dcp.buffer;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.dcp.message.ResponseStatus;
import rx.Single;

import static java.util.Objects.requireNonNull;

public interface DcpOps {

    /**
     * Thrown when response status field is non-zero.
     */
    class BadResponseStatusException extends CouchbaseException {
        private final ResponseStatus status;

        public BadResponseStatusException(ResponseStatus status) {
            super(status.toString());
            this.status = requireNonNull(status);
        }

        ResponseStatus status() {
            return status;
        }
    }

    Single<ObserveSeqnoResponse> observeSeqno(int partition, long vbuuid);

    Single<FailoverLogResponse> getFailoverLog(int partition);
}
