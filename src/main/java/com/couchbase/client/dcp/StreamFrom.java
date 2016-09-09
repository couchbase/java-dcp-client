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
package com.couchbase.client.dcp;

/**
 * From which point in time to start the DCP stream.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public enum StreamFrom {
    /**
     * Start at the very beginning - will stream all docs in the bucket.
     */
    BEGINNING,

    /**
     * Start "now", where now is a time point of execution in the running program where the state
     * is gathered from each partition. Mutations will be streamed after this point.
     */
    NOW
}
