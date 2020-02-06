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
package com.couchbase.client.dcp.transport.netty;

import com.couchbase.client.dcp.Credentials;
import com.couchbase.client.dcp.CredentialsProvider;
import com.couchbase.client.dcp.buffer.DcpBucketConfig;
import com.couchbase.client.dcp.config.ClientEnvironment;
import com.couchbase.client.dcp.config.SSLEngineFactory;
import com.couchbase.client.deps.io.netty.channel.Channel;
import com.couchbase.client.deps.io.netty.channel.ChannelInitializer;
import com.couchbase.client.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.deps.io.netty.handler.logging.LoggingHandler;
import com.couchbase.client.deps.io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.subjects.Subject;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Configures the pipeline for the HTTP config stream.
 *
 * @author Michael Nitschinger
 * @since 1.0.0
 */
public class ConfigPipeline extends ChannelInitializer<Channel> {

  /**
   * The logger used.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPipeline.class);

  /**
   * The stateful environment.
   */
  private final ClientEnvironment environment;

  /**
   * Address used to replace $HOST parts in the config when used against localhost.
   */
  private final InetSocketAddress address;

  /**
   * The name of the bucket
   */
  private final String bucket;

  /**
   * The config stream where the configs are emitted into.
   */
  private final Subject<DcpBucketConfig, DcpBucketConfig> configStream;

  /**
   * The revision of the last config emitted. Only emit a config
   * if it is newer than this revision.
   */
  private final AtomicLong currentBucketConfigRev;

  private final SSLEngineFactory sslEngineFactory;

  /**
   * Creates a new config pipeline.
   *
   * @param environment the stateful environment.
   * @param address address of the remote server.
   * @param configStream config stream where to send the configs.
   */
  public ConfigPipeline(final ClientEnvironment environment, final InetSocketAddress address,
                        final Subject<DcpBucketConfig, DcpBucketConfig> configStream,
                        final AtomicLong currentBucketConfigRev) {
    this.address = address;
    this.bucket = environment.bucket();
    this.configStream = configStream;
    this.currentBucketConfigRev = currentBucketConfigRev;
    this.environment = environment;
    if (environment.sslEnabled()) {
      this.sslEngineFactory = new SSLEngineFactory(environment);
    } else {
      this.sslEngineFactory = null;
    }
  }

  /**
   * Init the pipeline with the HTTP codec and our handler which does all the json chunk
   * decoding and parsing.
   * <p>
   * If trace logging is enabled also add the logging handler so we can figure out whats
   * going on when debugging.
   */
  @Override
  protected void initChannel(final Channel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    if (environment.sslEnabled()) {
      pipeline.addLast(new SslHandler(sslEngineFactory.get()));
    }
    if (LOGGER.isTraceEnabled()) {
      pipeline.addLast(new LoggingHandler(LogLevel.TRACE));
    }
    CredentialsProvider credentialsProvider = environment.credentialsProvider();
    Credentials credentials = credentialsProvider.get(address);
    pipeline
        .addLast(new HttpClientCodec())
        .addLast(new StartStreamHandler(bucket, credentials.getUsername(), credentials.getPassword()))
        .addLast(new ConfigHandler(configStream, currentBucketConfigRev, environment));
  }

}
