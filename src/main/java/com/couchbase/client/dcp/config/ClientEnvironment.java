package com.couchbase.client.dcp.config;

import com.couchbase.client.dcp.Client;
import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;

import java.util.List;

/**
 * Stateful environment for internal usage.
 */
public class ClientEnvironment {

    private final List<String> clusterAt;
    private final DataEventHandler dataEventHandler;
    private final ConnectionNameGenerator connectionNameGenerator;
    private final String bucket;
    private final String password;
    private final DcpControl dcpControl;
    private final EventLoopGroup eventLoopGroup;

    private ClientEnvironment(Builder builder) {
        clusterAt = builder.clusterAt;
        dataEventHandler = builder.dataEventHandler;
        connectionNameGenerator = builder.connectionNameGenerator;
        bucket = builder.bucket;
        password = builder.password;
        dcpControl = builder.dcpControl;
        eventLoopGroup = builder.eventLoopGroup;
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<String> clusterAt() {
        return clusterAt;
    }

    public DataEventHandler dataEventHandler() {
        return dataEventHandler;
    }

    public ConnectionNameGenerator connectionNameGenerator() {
        return connectionNameGenerator;
    }

    public String bucket() {
        return bucket;
    }

    public String password() {
        return password;
    }

    public DcpControl dcpControl() {
        return dcpControl;
    }

    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    public static class Builder {
        private List<String> clusterAt;
        private DataEventHandler dataEventHandler;
        private ConnectionNameGenerator connectionNameGenerator;
        private String bucket;
        private String password;
        private DcpControl dcpControl;
        private EventLoopGroup eventLoopGroup;

        public Builder setClusterAt(List<String> clusterAt) {
            this.clusterAt = clusterAt;
            return this;
        }

        public Builder setDataEventHandler(DataEventHandler dataEventHandler) {
            this.dataEventHandler = dataEventHandler;
            return this;
        }

        public Builder setConnectionNameGenerator(ConnectionNameGenerator connectionNameGenerator) {
            this.connectionNameGenerator = connectionNameGenerator;
            return this;
        }

        public Builder setBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDcpControl(DcpControl dcpControl) {
            this.dcpControl = dcpControl;
            return this;
        }

        public Builder setEventLoopGroup(EventLoopGroup eventLoopGroup) {
            this.eventLoopGroup = eventLoopGroup;
            return this;
        }

        public ClientEnvironment build() {
            return new ClientEnvironment(this);
        }

    }

}
