package com.couchbase.client.dcp;


import java.util.UUID;

public class DefaultConnectionNameGenerator implements ConnectionNameGenerator {

    public static final ConnectionNameGenerator INSTANCE = new DefaultConnectionNameGenerator();

    private DefaultConnectionNameGenerator() {}

    @Override
    public String name() {
        return "dcp-java-" + UUID.randomUUID().toString();
    }
}
