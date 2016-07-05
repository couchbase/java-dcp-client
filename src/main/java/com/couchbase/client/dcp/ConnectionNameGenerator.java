package com.couchbase.client.dcp;

public interface ConnectionNameGenerator {

    /**
     * Generate the name for a DCP Connection.
     */
    String name();
}
