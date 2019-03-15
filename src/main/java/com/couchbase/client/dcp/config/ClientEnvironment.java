/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

import com.couchbase.client.core.env.ConfigParserEnvironment;
import com.couchbase.client.core.env.CoreScheduler;
import com.couchbase.client.core.env.resources.NoOpShutdownHook;
import com.couchbase.client.core.env.resources.ShutdownHook;
import com.couchbase.client.core.event.CouchbaseEvent;
import com.couchbase.client.core.event.EventBus;
import com.couchbase.client.core.event.EventType;
import com.couchbase.client.core.node.DefaultMemcachedHashingStrategy;
import com.couchbase.client.core.node.MemcachedHashingStrategy;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.dcp.ConnectionNameGenerator;
import com.couchbase.client.dcp.ControlEventHandler;
import com.couchbase.client.dcp.CredentialsProvider;
import com.couchbase.client.dcp.DataEventHandler;
import com.couchbase.client.dcp.SystemEventHandler;
import com.couchbase.client.dcp.buffer.PersistedSeqnos;
import com.couchbase.client.dcp.buffer.StreamEventBuffer;
import com.couchbase.client.dcp.events.DefaultDcpEventBus;
import com.couchbase.client.deps.io.netty.channel.EventLoopGroup;
import com.couchbase.client.deps.io.netty.util.concurrent.Future;
import com.couchbase.client.deps.io.netty.util.concurrent.GenericFutureListener;
import rx.Completable;
import rx.CompletableSubscriber;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The {@link ClientEnvironment} is responsible to carry various configuration and
 * state information throughout the lifecycle.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class ClientEnvironment implements SecureEnvironment, ConfigParserEnvironment {
  public static final long DEFAULT_BOOTSTRAP_TIMEOUT = TimeUnit.SECONDS.toMillis(5);
  public static final long DEFAULT_CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
  public static final Delay DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY = Delay.linear(TimeUnit.SECONDS, 10, 1);
  public static final int DEFAULT_CONFIG_PROVIDER_RECONNECT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  public static final long DEFAULT_SOCKET_CONNECT_TIMEOUT = TimeUnit.SECONDS.toMillis(1);
  public static final Delay DEFAULT_DCP_CHANNELS_RECONNECT_DELAY = Delay.fixed(200, TimeUnit.MILLISECONDS);
  public static final int DEFAULT_DCP_CHANNELS_RECONNECT_MAX_ATTEMPTS = Integer.MAX_VALUE;
  public static final boolean DEFAULT_SSL_ENABLED = false;
  public static final int BOOTSTRAP_HTTP_DIRECT_PORT = 8091;
  public static final int BOOTSTRAP_HTTP_SSL_PORT = 18091;

  /**
   * Stores the list of bootstrap nodes (where the cluster is).
   */
  private final List<InetSocketAddress> clusterAt;

  /**
   * Stores the generator for each DCP connection name.
   */
  private final ConnectionNameGenerator connectionNameGenerator;

  /**
   * The name of the bucket.
   */
  private final String bucket;

  /**
   * The connecting credentialsProvider.
   */
  private final CredentialsProvider credentialsProvider;

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
   * How long to wait between each call to observeSeqnos.
   * Values <= 0 disable persistence polling.
   */
  private final long persistencePollingIntervalMillis;

  /**
   * User-attached data event handler.
   */
  private volatile DataEventHandler dataEventHandler;

  /**
   * User-attached control event handler.
   */
  private volatile ControlEventHandler controlEventHandler;

  /**
   * Bookkeeper for observed seqno persistence.
   */
  private final PersistedSeqnos persistedSeqnos = PersistedSeqnos.uninitialized();

  /**
   * Delay strategy for configuration provider reconnection.
   */
  private final Delay configProviderReconnectDelay;

  /**
   * Maximum number of attempts to reconnect configuration provider before giving up.
   */
  private final int configProviderReconnectMaxAttempts;

  /**
   * Delay strategy for configuration provider reconnection.
   */
  private final Delay dcpChannelsReconnectDelay;

  /**
   * Maximum number of attempts to reconnect configuration provider before giving up.
   */
  private final int dcpChannelsReconnectMaxAttempts;

  private final EventBus eventBus;
  private final Scheduler scheduler;
  private final ShutdownHook schedulerShutdownHook;
  private Subscription systemEventSubscription;
  private final boolean sslEnabled;
  private final String sslKeystoreFile;
  private final String sslKeystorePassword;
  private final KeyStore sslKeystore;
  private final int bootstrapHttpDirectPort;
  private final int bootstrapHttpSslPort;

  /**
   * Creates a new environment based on the builder.
   *
   * @param builder the builder to build the environment.
   */
  private ClientEnvironment(final Builder builder) {
    connectionNameGenerator = builder.connectionNameGenerator;
    bucket = builder.bucket;
    credentialsProvider = builder.credentialsProvider;
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
    dcpChannelsReconnectDelay = builder.dcpChannelsReconnectDelay;
    dcpChannelsReconnectMaxAttempts = builder.dcpChannelsReconnectMaxAttempts;
    if (builder.eventBus != null) {
      eventBus = builder.eventBus;
      this.scheduler = null;
      this.schedulerShutdownHook = new NoOpShutdownHook();
    } else {
      CoreScheduler scheduler = new CoreScheduler(3);
      this.scheduler = scheduler;
      this.schedulerShutdownHook = scheduler;
      eventBus = new DefaultDcpEventBus(scheduler);
    }
    bootstrapHttpDirectPort = builder.bootstrapHttpDirectPort;
    bootstrapHttpSslPort = builder.bootstrapHttpSslPort;
    sslEnabled = builder.sslEnabled;
    sslKeystoreFile = builder.sslKeystoreFile;
    sslKeystorePassword = builder.sslKeystorePassword;
    sslKeystore = builder.sslKeystore;
    clusterAt = builder.clusterAt;
    persistencePollingIntervalMillis = builder.persistencePollingIntervalMillis;

    if (persistencePollingIntervalMillis > 0) {
      if (bufferAckWatermark == 0) {
        throw new IllegalArgumentException("Rollback mitigation requires flow control.");
      }

      final StreamEventBuffer buffer = new StreamEventBuffer(eventBus);
      dataEventHandler = buffer;
      controlEventHandler = buffer;
    }
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
  public List<InetSocketAddress> clusterAt() {
    return clusterAt;
  }

  /**
   * Returns the currently attached data event handler.
   */
  public DataEventHandler dataEventHandler() {
    return dataEventHandler;
  }

  /**
   * Returns the stream event buffer used for rollback mitigation.
   *
   * @throws IllegalStateException if persistence polling / rollback mitigation is disabled
   */
  public StreamEventBuffer streamEventBuffer() {
    try {
      return (StreamEventBuffer) dataEventHandler;
    } catch (ClassCastException e) {
      throw new IllegalStateException("Stream event buffer not configured");
    }
  }

  /**
   * Returns the bookkeeper for observed seqno persistence.
   */
  public PersistedSeqnos persistedSeqnos() {
    return persistedSeqnos;
  }

  /**
   * Returns the interval between observeSeqno requests.
   * Values <= 0 disable persistence polling.
   */
  public long persistencePollingIntervalMillis() {
    return persistencePollingIntervalMillis;
  }

  /**
   * Returns true if and only if rollback mitigation / persistence polling is enabled.
   */
  public boolean persistencePollingEnabled() {
    return persistencePollingIntervalMillis > 0;
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
   * The credentials provider for the connection
   */
  public CredentialsProvider credentialsProvider() {
    return credentialsProvider;
  }

  /**
   * Username used.
   *
   * @deprecated Use {@link ClientEnvironment#credentialsProvider()} instead to access username
   */
  @Deprecated
  public String username() {
    return credentialsProvider.get(null).getUsername();
  }

  /**
   * Password of the user.
   *
   * @deprecated Use {@link ClientEnvironment#credentialsProvider()} instead to access password
   */
  @Deprecated
  public String password() {
    return credentialsProvider.get(null).getPassword();
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
    if (persistencePollingEnabled()) {
      streamEventBuffer().setDataEventHandler(dataEventHandler);
    } else {
      this.dataEventHandler = dataEventHandler;
    }
  }

  /**
   * Set/Override the control event handler.
   */
  public void setControlEventHandler(ControlEventHandler controlEventHandler) {
    if (persistencePollingEnabled()) {
      streamEventBuffer().setControlEventHandler(controlEventHandler);
    } else {
      this.controlEventHandler = controlEventHandler;
    }
  }

  /**
   * Set/Override the control event handler.
   */
  public void setSystemEventHandler(final SystemEventHandler systemEventHandler) {
    if (systemEventSubscription != null) {
      systemEventSubscription.unsubscribe();
    }
    if (systemEventHandler != null) {
      systemEventSubscription = eventBus().get()
          .filter(evt -> evt.type().equals(EventType.SYSTEM))
          .subscribe(new Subscriber<CouchbaseEvent>() {
            @Override
            public void onCompleted() { /* Ignoring on purpose. */}

            @Override
            public void onError(Throwable e) { /* Ignoring on purpose. */ }

            @Override
            public void onNext(CouchbaseEvent evt) {
              systemEventHandler.onEvent(evt);
            }
          });
    }
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

  /**
   * Returns the event bus where events are broadcasted on and can be published to.
   */
  public EventBus eventBus() {
    return eventBus;
  }

  public int bootstrapHttpDirectPort() {
    return bootstrapHttpDirectPort;
  }

  public int bootstrapHttpSslPort() {
    return bootstrapHttpSslPort;
  }

  @Override
  public boolean sslEnabled() {
    return sslEnabled;
  }

  @Override
  public String sslKeystoreFile() {
    return sslKeystoreFile;
  }

  @Override
  public String sslKeystorePassword() {
    return sslKeystorePassword;
  }

  @Override
  public KeyStore sslKeystore() {
    return sslKeystore;
  }

  @Override
  public MemcachedHashingStrategy memcachedHashingStrategy() {
    // This is hardcoded, because memcached nodes do not support DCP anyway.
    return DefaultMemcachedHashingStrategy.INSTANCE;
  }

  public static class Builder {
    private List<InetSocketAddress> clusterAt;
    private ConnectionNameGenerator connectionNameGenerator;
    private String bucket;
    private CredentialsProvider credentialsProvider;
    private long bootstrapTimeout = DEFAULT_BOOTSTRAP_TIMEOUT;
    private long connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private DcpControl dcpControl;
    private EventLoopGroup eventLoopGroup;
    private boolean eventLoopGroupIsPrivate;
    private boolean poolBuffers;
    private Delay configProviderReconnectDelay = DEFAULT_CONFIG_PROVIDER_RECONNECT_DELAY;
    private int configProviderReconnectMaxAttempts = DEFAULT_CONFIG_PROVIDER_RECONNECT_MAX_ATTEMPTS;
    private long socketConnectTimeout = DEFAULT_SOCKET_CONNECT_TIMEOUT;
    private Delay dcpChannelsReconnectDelay = DEFAULT_DCP_CHANNELS_RECONNECT_DELAY;
    private int dcpChannelsReconnectMaxAttempts = DEFAULT_DCP_CHANNELS_RECONNECT_MAX_ATTEMPTS;
    private int bootstrapHttpDirectPort = BOOTSTRAP_HTTP_DIRECT_PORT;
    private int bootstrapHttpSslPort = BOOTSTRAP_HTTP_SSL_PORT;

    private int bufferAckWatermark;
    private EventBus eventBus;
    private boolean sslEnabled = DEFAULT_SSL_ENABLED;
    private String sslKeystoreFile;
    private String sslKeystorePassword;
    private KeyStore sslKeystore;
    private long persistencePollingIntervalMillis;

    public Builder setClusterAt(List<InetSocketAddress> clusterAt) {
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

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
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

    public Builder setDcpChannelsReconnectDelay(Delay dcpChannelsReconnectDelay) {
      this.dcpChannelsReconnectDelay = dcpChannelsReconnectDelay;
      return this;
    }

    public Builder setDcpChannelsReconnectMaxAttempts(int dcpChannelsReconnectMaxAttempts) {
      this.dcpChannelsReconnectMaxAttempts = dcpChannelsReconnectMaxAttempts;
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

    public Builder setEventBus(EventBus eventBus) {
      this.eventBus = eventBus;
      return this;
    }

    /**
     * If SSL not enabled, sets the port to use for HTTP bootstrap
     * (default value {@value #BOOTSTRAP_HTTP_DIRECT_PORT}).
     */
    public Builder setBootstrapHttpDirectPort(final int bootstrapHttpDirectPort) {
      this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
      return this;
    }

    /**
     * If SSL enabled, sets the port to use for HTTP bootstrap
     * (default value {@value #BOOTSTRAP_HTTP_SSL_PORT}).
     */
    public Builder setBootstrapHttpSslPort(final int bootstrapHttpSslPort) {
      this.bootstrapHttpSslPort = bootstrapHttpSslPort;
      return this;
    }

    /**
     * Set if SSL should be enabled (default value {@value #DEFAULT_SSL_ENABLED}).
     * If true, also set {@link #setSslKeystoreFile(String)} and {@link #setSslKeystorePassword(String)}.
     */
    public Builder setSslEnabled(final boolean sslEnabled) {
      this.sslEnabled = sslEnabled;
      return this;
    }

    /**
     * Defines the location of the SSL Keystore file (default value null, none).
     * <p>
     * You can either specify a file or the keystore directly via {@link #setSslKeystore(KeyStore)}. If the explicit
     * keystore is used it takes precedence over the file approach.
     */
    public Builder setSslKeystoreFile(final String sslKeystoreFile) {
      this.sslKeystoreFile = sslKeystoreFile;
      return this;
    }

    /**
     * Sets the SSL Keystore password to be used with the Keystore file (default value null, none).
     *
     * @see #setSslKeystoreFile(String)
     */
    public Builder setSslKeystorePassword(final String sslKeystorePassword) {
      this.sslKeystorePassword = sslKeystorePassword;
      return this;
    }

    /**
     * Sets the SSL Keystore directly and not indirectly via filepath.
     * <p>
     * You can either specify a file or the keystore directly via {@link #setSslKeystore(KeyStore)}. If the explicit
     * keystore is used it takes precedence over the file approach.
     *
     * @param sslKeystore the keystore to use.
     */
    public Builder setSslKeystore(final KeyStore sslKeystore) {
      this.sslKeystore = sslKeystore;
      return this;
    }

    /**
     * Sets the interval for persistence polling to mitigate rollbacks. Values <= 0 disable polling.
     */
    public Builder setPersistencePollingIntervalMillis(long persistencePollingIntervalMillis) {
      this.persistencePollingIntervalMillis = persistencePollingIntervalMillis;
      return this;
    }

    public ClientEnvironment build() {
      int defaultConfigPort = sslEnabled ? bootstrapHttpSslPort : bootstrapHttpDirectPort;
      for (int i = 0; i < clusterAt.size(); i++) {
        InetSocketAddress node = clusterAt.get(i);
        if (node.getPort() == 0) {
          clusterAt.set(i, new InetSocketAddress(node.getHostName(), defaultConfigPort));
        }
      }
      return new ClientEnvironment(this);
    }
  }

  /**
   * Shut down this stateful environment.
   * <p>
   * Note that it will only release/terminate resources which are owned by the client,
   * especially if a custom event loop group is passed in it needs to be shut down
   * separately.
   *
   * @return a {@link Completable} indicating completion of the shutdown process.
   */
  @SuppressWarnings({"unchecked"})
  public Completable shutdown() {
    Observable<Boolean> loopShutdown = Observable.empty();

    if (eventLoopGroupIsPrivate) {
      loopShutdown = Completable.create(new Completable.OnSubscribe() {
        @Override
        public void call(final CompletableSubscriber subscriber) {
          eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.MILLISECONDS).addListener(
              new GenericFutureListener() {
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
      }).toObservable();
    }
    return Observable.merge(schedulerShutdownHook.shutdown(), loopShutdown)
        .reduce(true, (previous, current) -> previous && current)
        .toCompletable();
  }

  @Override
  public String toString() {
    return "ClientEnvironment{" +
        "clusterAt=" + clusterAt +
        ", connectionNameGenerator=" + connectionNameGenerator.getClass().getSimpleName() +
        ", bucket='" + bucket + '\'' +
        ", dcpControl=" + dcpControl +
        ", eventLoopGroup=" + eventLoopGroup.getClass().getSimpleName() +
        ", eventLoopGroupIsPrivate=" + eventLoopGroupIsPrivate +
        ", poolBuffers=" + poolBuffers +
        ", bufferAckWatermark=" + bufferAckWatermark +
        ", connectTimeout=" + connectTimeout +
        ", bootstrapTimeout=" + bootstrapTimeout +
        ", sslEnabled=" + sslEnabled +
        ", sslKeystoreFile='" + sslKeystoreFile + '\'' +
        ", sslKeystorePassword=" + (sslKeystorePassword != null && !sslKeystorePassword.isEmpty()) +
        ", sslKeystore=" + sslKeystore +
        '}';
  }

  public Delay dcpChannelsReconnectDelay() {
    return dcpChannelsReconnectDelay;
  }

  public int dcpChannelsReconnectMaxAttempts() {
    return dcpChannelsReconnectMaxAttempts;
  }
}
