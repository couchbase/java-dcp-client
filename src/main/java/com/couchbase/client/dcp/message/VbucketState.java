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
package com.couchbase.client.dcp.message;

public enum VbucketState {
    /**
     * Any live state (except DEAD).
     */
    ANY(0),
    /**
     * Actively servicing a partition.
     */
    ACTIVE(1),
    /**
     * Servicing a partition as a replica only.
     */
    REPLICA(2),
    /**
     * Pending active.
     */
    PENDING(3),
    /**
     * Not in use, pending deletion.
     */
    DEAD(4);

    private final int value;

    VbucketState(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
