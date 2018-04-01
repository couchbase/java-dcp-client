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

/**
 * Immutable.
 */
class PartitionInstance {

    // The vbucket number.
    private final short partition;

    // The "slot" is an index assigned to an instance of the partition.
    // When replication is enabled, there are multiple copies of a partition,
    // each hosted on a different node. Each copy (instance) has an index
    // in the bucket config. The active instance has slot 0, the zero-th replica has slot 1, and so on.
    private final int slot;

    PartitionInstance(final short partition, final int slot) {
        this.partition = partition;
        this.slot = slot;
    }

    short partition() {
        return partition;
    }

    int slot() {
        return slot;
    }

    @Override
    public String toString() {
        return partition + "/" + slot;
    }
}
