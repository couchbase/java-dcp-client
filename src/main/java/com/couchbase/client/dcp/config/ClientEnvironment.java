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
package com.couchbase.client.dcp.config;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The {@link ClientEnvironment} is responsible to carry various configuration and
 * state information throughout the lifecycle.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class ClientEnvironment {
    public static final long DEFAULT_BOOTSTRAP_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
    public static final long DEFAULT_CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    public static final Delay DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY = Delay.linear(TimeUnit.SECONDS, 10, 1);
    public static final int DEFAULT_CONFIG_PROVIDER_RECONNECT_MAX_ATTEMPTS = Integer.MAX_VALUE;
    public static final long DEFAULT_SOCKET_CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

    /**
     * Stores the list of bootstrap nodes (where the cluster is).
     */
    private final List<String> clusterAt;

    /**
     * Stores the generator for each DCP connection name.
     */
    private final ConnectionNameGenerator connectionNameGenerator;

    /**
     * The name of the bucket.
     */
    private final String bucket;

    /**
     * The password of the bucket.
     */
    private final String password;

    /**
     * Time in milliseconds to wait for initial configuration during bootstrap.
     */
    private final long bootstrapTimeout;

    /**
     * Time in milliseconds to wait configuration provider socket to connect.
     */
    private final long connectTimeout;

    /**
     * DCP control params, optional.
     */
    private final DcpControl dcpControl;

    /**
     * The IO loops.
     */
    private final EventLoopGroup eventLoopGroup;

    /**
     * If the client instantiated the IO loops (it is then responsible for shutdown).
     */
    private final boolean eventLoopGroupIsPrivate;

    /**
     * If buffer pooling is enabled throughout the client.
     */
    private final boolean poolBuffers;

    /**
     * What the buffer ack watermark in percent should be.
     */
    private final int bufferAckWatermark;

    /**
     * Socket connect timeout in milliseconds.
     */
    private final long socketConnectTimeout;

    /**
     * User-attached data event handler.
     */
    private volatile DataEventHandler dataEventHandler;

    /**
     * User-attached control event handler.
     */
    private volatile ControlEventHandler controlEventHandler;

    /**
     * Delay strategy for configuration provider reconnection.
     */
    private final Delay configProviderReconnectDelay;

    /**
     * Maximum number of attempts to reconnect configuration provider before giving up.
     */
    private final int configProviderReconnectMaxAttempts;

    /**
     * Creates a new environment based on the builder.
     *
     * @param builder the builder to build the environment.
     */
    private ClientEnvironment(final Builder builder) {
        clusterAt = builder.clusterAt;
        connectionNameGenerator = builder.connectionNameGenerator;
        bucket = builder.bucket;
        password = builder.password;
        bootstrapTimeout = builder.bootstrapTimeout;
        connectTimeout = builder.connectTimeout;
        dcpControl = builder.dcpControl;
        eventLoopGroup = builder.eventLoopGroup;
        eventLoopGroupIsPrivate = builder.eventLoopGroupIsPrivate;
        bufferAckWatermark = builder.bufferAckWatermark;
        poolBuffers = builder.poolBuffers;
        configProviderReconnectDelay = builder.configProviderReconnectDelay;
        configProviderReconnectMaxAttempts = builder.configProviderReconnectMaxAttempts;
        socketConnectTimeout = builder.socketConnectTimeout;
    }

    /**
     * Returns a new {@link Builder} to craft a {@link ClientEnvironment}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Lists the bootstrap nodes.
     */
    public List<String> clusterAt() {
        return clusterAt;
    }

    /**
     * Returns the currently attached data event handler.
     */
    public DataEventHandler dataEventHandler() {
        return dataEventHandler;
    }

    /**
     * Returns the current attached control event handler.
     */
    public ControlEventHandler controlEventHandler() {
        return controlEventHandler;
    }

    /**
     * Returns the name generator used to identify DCP sockets.
     */
    public ConnectionNameGenerator connectionNameGenerator() {
        return connectionNameGenerator;
    }

    /**
     * Name of the bucket used.
     */
    public String bucket() {
        return bucket;
    }

    /**
     * Password of the bucket used.
     */
    public String password() {
        return password;
    }

    /**
     * Returns all DCP control params set, may be empty.
     */
    public DcpControl dcpControl() {
        return dcpControl;
    }

    /**
     * The watermark in percent for buffer acknowledgements.
     */
    public int bufferAckWatermark() {
        return bufferAckWatermark;
    }

    /**
     * Returns the currently attached event loop group for IO process.ing.
     */
    public EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    /**
     * Time in milliseconds to wait for first configuration during bootstrap.
     */
    public long bootstrapTimeout() {
        return bootstrapTimeout;
    }

    /**
     * Time in milliseconds to wait configuration provider socket to connect.
     */
    public long connectTimeout() {
        return connectTimeout;
    }

    /**
     * Set/Override the data event handler.
     */
    public void setDataEventHandler(DataEventHandler dataEventHandler) {
        this.dataEventHandler = dataEventHandler;
    }

    /**
     * Set/Override the control event handler.
     */
    public void setControlEventHandler(ControlEventHandler controlEventHandler) {
        this.controlEventHandler = controlEventHandler;
    }

    /**
     * If buffer pooling is enabled.
     */
    public boolean poolBuffers() {
        return poolBuffers;
    }

    /**
     * Delay strategy for configuration provider reconnection.
     */
    public Delay configProviderReconnectDelay() {
        return configProviderReconnectDelay;
    }

    /**
     * Maximum number of attempts to reconnect configuration provider before giving up.
     */
    public int configProviderReconnectMaxAttempts() {
        return configProviderReconnectMaxAttempts;
    }

    /**
     * Socket connect timeout in milliseconds.
     */
    public long socketConnectTimeout() {
        return socketConnectTimeout;
    }

    public static class Builder {
        private List<String> clusterAt;
        private ConnectionNameGenerator connectionNameGenerator;
        private String bucket;
        private String password;
        private long bootstrapTimeout = DEFAULT_BOOTSTRAP_TIMEOUT;
        private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
        private DcpControl dcpControl;
        private EventLoopGroup eventLoopGroup;
        private boolean eventLoopGroupIsPrivate;
        private boolean poolBuffers;
        private Delay configProviderReconnectDelay = DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY;
        private int configProviderReconnectMaxAttempts = DEFAULT_CONFIG_PROVIDER_RECONNECT_MAX_ATTEMPTS;
        private long socketConnectTimeout = DEFAULT_SOCKET_CONNECT_TIMEOUT;

        private int bufferAckWatermark;

        public Builder setClusterAt(List<String> clusterAt) {
            this.clusterAt = clusterAt;
            return this;
        }

        public Builder setBufferAckWatermark(int watermark) {
            this.bufferAckWatermark = watermark;
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

        public Builder setBootstrapTimeout(long bootstrapTimeout) {
            this.bootstrapTimeout = bootstrapTimeout;
            return this;
        }

        public Builder setConnectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder setConfigProviderReconnectDelay(Delay configProviderReconnectDelay) {
            this.configProviderReconnectDelay = configProviderReconnectDelay;
            return this;
        }

        public Builder setConfigProviderReconnectMaxAttempts(int configProviderReconnectMaxAttempts) {
            this.configProviderReconnectMaxAttempts = configProviderReconnectMaxAttempts;
            return this;
        }

        /**
         * Sets a custom socket connect timeout.
         *
         * @param socketConnectTimeout the socket connect timeout in milliseconds.
         */
        public Builder setSocketConnectTimeout(long socketConnectTimeout) {
            this.socketConnectTimeout = socketConnectTimeout;
            return this;
        }

        public Builder setDcpControl(DcpControl dcpControl) {
            this.dcpControl = dcpControl;
            return this;
        }

        public Builder setEventLoopGroup(EventLoopGroup eventLoopGroup, boolean priv) {
            this.eventLoopGroup = eventLoopGroup;
            this.eventLoopGroupIsPrivate = priv;
            return this;
        }

        public Builder setBufferPooling(boolean pool) {
            this.poolBuffers = pool;
            return this;
        }

        public ClientEnvironment build() {
            return new ClientEnvironment(this);
        }

    }

    /**
     * Shut down this stateful environment.
     *
     * Note that it will only release/terminate resources which are owned by the client,
     * especially if a custom event loop group is passed in it needs to be shut down
     * separately.
     *
     * @return a {@link Completable} indicating completion of the shutdown process.
     */
    @SuppressWarnings({"unchecked"})
    public Completable shutdown() {
        if (!eventLoopGroupIsPrivate) {
            return Completable.complete();
        }

        return Completable.create(new Completable.CompletableOnSubscribe() {
            @Override
            public void call(final Completable.CompletableSubscriber subscriber) {
                eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS).addListener(new GenericFutureListener() {
                    @Override
                    public void operationComplete(Future future) throws Exception {
                        if (future.isSuccess()) {
                            subscriber.onCompleted();
                        } else {
                            subscriber.onError(future.cause());
                        }
                    }
                });
            }
        });
    }

    @Override
    public String toString() {
        return "ClientEnvironment{" +
            "clusterAt=" + clusterAt +
            ", connectionNameGenerator=" + connectionNameGenerator.getClass().getSimpleName() +
            ", bucket='" + bucket + '\'' +
            ", passwordSet=" + !password.isEmpty() +
            ", dcpControl=" + dcpControl +
            ", eventLoopGroup=" + eventLoopGroup.getClass().getSimpleName() +
            ", eventLoopGroupIsPrivate=" + eventLoopGroupIsPrivate +
            ", poolBuffers=" + poolBuffers +
            ", bufferAckWatermark=" + bufferAckWatermark +
            ", connectTimeout=" + connectTimeout +
            ", bootstrapTimeout=" + bootstrapTimeout +
            '}';
    }
}
